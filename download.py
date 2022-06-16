from datetime import datetime, timedelta
from time import sleep
import pandas as pd
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from pytz import timezone
from subprocess import check_output, CalledProcessError

import requests
from requests.adapters import HTTPAdapter
from requests.adapters import HTTPAdapter, Retry
from requests.packages import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from rich.console import Console
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('n', nargs='?', const=0, type=int, default=1)
args = parser.parse_args()
console = Console()


def handleGitCommit(m):
    try:
        check_output("git add .", shell=True)
        s = check_output(f"git commit -m \"{m}\"", shell=True).decode()
        return f"[green]Successfully Committed:[/green] {s}"
    except CalledProcessError as e:
        return f"[red]Error: {e.output.decode()}[/]"


def handleGitPull():
    try:
        s = check_output("git pull origin main", shell=True).decode()
        return f"[green]Pulled From Remote Repo:[/green] {s}"
    except CalledProcessError as e:
        return f"[red]Error: {e.output.decode()}[/]"


def handleGitPush():
    try:
        s = check_output("git push origin main", shell=True).decode()
        return f"[green]Pushed To Remote Repo,[/green] {s}"
    except CalledProcessError as e:
        return f"[red]Error: {e.output.decode()}[/]"


def withLoader(cb, message="", spinner='aesthetic'):
    done = False
    returns = None
    with console.status(f"[bold yellow] {message}...", spinner=spinner) as s:
        while not done:
            returns = cb()
            done = True
    return returns


def withLoaderWithParam(cb, param, message="", spinner='aesthetic'):
    done = False
    returns = None
    with console.status(f"[bold yellow] {message}...", spinner=spinner) as s:
        while not done:
            returns = cb(*param)
            done = True
    return returns


def merge_dicts(*dict_args):
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


# import ssl
# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.poolmanager import PoolManager
# from urllib3.util import ssl_
# CIPHERS = "ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-SHA384:ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-SHA256:AES256-SHA"
# class TLSAdapter(HTTPAdapter):

#     def __init__(self, ssl_options=0, *args, **kwargs):
#         self.ssl_options = ssl_options
#         super().__init__(*args, **kwargs)

#     def init_poolmanager(self, *args, **kwargs):
#         context = ssl_.create_urllib3_context(
#             ciphers=CIPHERS, cert_reqs=ssl.CERT_REQUIRED, options=self.ssl_options)
#         self.poolmanager = PoolManager(*args, ssl_context=context, **kwargs)


# def download(url, fileName):
#     headers = {
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36 Edg/101.0.1210.53",
#     }
#     adapter = TLSAdapter(ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1)

#     with requests.session() as session:
#         # session.mount("https://www.researchgate.net/", adapter)
#         session.mount('http://', adapter)
#         session.mount('https://', adapter)

#         response = session.get(url, headers=headers)
#         # print(response.status_code)  # 200
#         if response.status_code == 200:
#             with open(f"data/papers_pdf/{fileName}.pdf", 'wb') as f:
#                 f.write(response.content)
#             return 0, 0
#         else:
#             return response.status_code, response.reason


def downloadSS(url, fName):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36 Edg/101.0.1210.53",
    }
    retry_strategy = Retry(
        total=3,
        backoff_factor=.5,
        # backoff_factor to apply between attempts after the second try (most errors are resolved immediately by a second try without a delay)
        status_forcelist=[403, 406, 429, 500, 502, 503, 504],
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    res = session.get(url, headers=headers, verify=False, timeout=30)
    if res.status_code == 200:
        with open(f"data/papers_pdf/{fName}.pdf", "wb") as f:
            f.write(res.content)
        session.close()
        return 0, 0
    else:
        session.close()
        return res.status_code, res.reason


def downloadManager():
    n = args.n
    inf = pd.read_csv("data/info/papers.csv")
    dl = pd.read_csv("data/info/processed.csv")
    next_dl = inf[~inf['uuid'].isin(dl['paper_id'])][:n]
    next_dl_dict = next_dl.to_dict('records')
    console.print(f"Downloading {n} papers")

    for d in next_dl_dict:
        paper_id = d['paper_id']
        uuid = d['uuid']

        pDf = pd.DataFrame([{
                            'uuid': uuid,
                            'paper_id': paper_id,
                            }])
        pDf.to_csv("data/info/processed.csv",
                   index=False, header=False, mode="a")

        download_links = d['download_link']
        title = d['title']
        links = download_links.split(";")
        print()
        success = False
        errors = []
        console.log(title)
        if links[0] != "No links found":
            console.log(f"[blue_violet]available links: {str(len(links))}[/]")

        for i, link in enumerate(links):
            if link == 'No links found':
                nf = pd.DataFrame([d])
                nf.to_csv("data/info/not_found.csv",
                          index=False, header=False, mode="a")
                console.log(f"[yellow]No links available[/]")
            else:
                # already downloaded this paper before
                done = pd.read_csv("data/info/done.csv")
                paperIds = done['paper_id'].unique().tolist()
                if paper_id in paperIds:
                    pDf.to_csv("data/info/done.csv",
                               index=False, header=False, mode="a")
                    console.log("[green]already downloaded[/]")
                    success = True
                if success:
                    break
                # download the paper
                else:
                    console.print(
                        f"[blue]-> Downloading with link[{i}]:[/] {link}")
                    try:
                        # code, reason = downloadSS(link, paper_id)
                        code, reason = withLoaderWithParam(
                            downloadSS, [link, paper_id], "Downloading...", 'dots')
                        if code == 0:
                            pDf.to_csv("data/info/done.csv",
                                       index=False, header=False, mode="a")
                            console.log("[green]downloaded[/]")
                            success = True
                            # no need to try other links
                            break
                        else:
                            success = False
                            console.log(f"[red]Error:{code} {reason}[/]")
                            errDict = merge_dicts(
                                d, {'code': code, 'reason': e})
                            errors.append(errDict)

                    except Exception as e:
                        errDict = merge_dicts(
                            d, {'code': None, 'reason': e})
                        errors.append(errDict)
                        console.log(f"[red]Error:{e}[/]")
                        success = False
        if not success:
            dlDf = pd.DataFrame(errors)
            dlDf.to_csv("data/info/errors.csv",
                        index=False, header=False, mode="a")
            console.log("[red]Failed to download[/]")


# downloadManager()


def startJob():
    msg = withLoader(handleGitPull, message="Pulling from remote repo")
    console.log(msg)

    downloaded = pd.read_csv("data/info/done.csv")
    preLen = len(downloaded)

    sched = BlockingScheduler(timezone=timezone('Asia/Dhaka'))

    sched.add_job(downloadManager, 'interval',
                  minutes=5,
                  #   seconds=2,
                  next_run_time=datetime.now(),  # start immediately
                  #   end_date=datetime.now() + timedelta(hours=2),
                  id='my_job_id')

    def execution_listener(event):
        if event.exception:
            print('The job crashed')
            return

        # console.log('The job executed successfully')
        job = sched.get_job(event.job_id)
        print()
        try:
            if job.id == 'my_job_id':
                downloadedPost = pd.read_csv("data/info/done.csv")
                postLen = len(downloadedPost)
                handleGitCommit(f"{(postLen - preLen)} papers downloaded")
                # msg = withLoader(
                #     handleGitPush, message="Pushing to the remote repo")

                console.log("Pushing to the remote repo")
                handleGitPush()

                print()

                print("Push Done!")
                # console.log("Push Done!")

                print(
                    f"Next Job scheduled to be run at: {job.next_run_time}")
                # check if there is any paper in the queu

        except:
            # all the jobs are done
            downloadedPost = pd.read_csv("data/info/done.csv")
            postLen = len(downloadedPost)
            handleGitCommit(f"{(postLen - preLen)} papers downloaded")
            msg = withLoader(
                handleGitPush, message="Pushing to the remote repo")
            console.log("Push Done!")
            sched.shutdown(wait=False)

    sched.print_jobs()
    sched.add_listener(execution_listener,
                       EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    sched.start()


startJob()

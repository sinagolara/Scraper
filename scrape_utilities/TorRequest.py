
### Requirements: Install Tor. Make sure torrc file (~/Library/Application Support/TorBrowser-Data/Tor/torrc) includes
### the strings "ControlPort 9051" [to activate the controller port for stem] and "CookieAuthentication 1" to authenticate with no password

import time
from stem.control import Controller
from stem import Signal
import requests
import random
from datetime import datetime
import collections
from fake_useragent import UserAgent
from multiprocessing import Pool
from cleaning.file_management import pattern_merge
import pandas as pd
from pandas.errors import EmptyDataError
from glob import glob
import os
import shutil
from pathlib import Path

# If you want different IPs for each connection, you can also use Stream Isolation over SOCKS by specifying a different proxy username:password combination for each connection.
# With this method, you only need one Tor instance and each requests client can use a different stream with a different exit node.
# In order to set this up, add unique proxy credentials for each requests.session object like so: socks5h://username:password@localhost:9050
# Tor Browser isolates streams on a per-domain basis by setting the credentials to firstpartydomain:randompassword, where randompassword is a random nonce for each unique first party domain.
# If you're crawling the same site and you want random IP's, then use a random username:password combination for each session. If you are crawling random domains and want to use the same circuit for requests to a domain, use Tor Browser's method of domain:randompassword for credentials.


def get_ip(session):
    # r = session.get("http://checkip.dyn.com")  ### returns the current IP
    # soup = BeautifulSoup(r.content, 'lxml')
    # ip = soup.find("body").text.replace('Current IP Address: ','')
    ip = session.get("http://icanhazip.com").text
    #print(ip)
    return ip

def ip_reuse_count(ip):
    time_stamp = datetime.now().strftime('%d-%H')  # day of month/hour
    repo = f'./ip_check/ip_repo/{time_stamp}.txt'
    if os.path.exists(repo):
        with open(repo, mode='r') as f:
            contents = f.read()
            ip_list = contents.split(',')
            N = ip_list.count(ip)
    else:
        N = 0
    return N

def store_ip(ip):
    time_stamp = datetime.now().strftime('%d-%H')  # day of month/hour/minute
    repo = f'./ip_check/ip_repo/{time_stamp}.txt'
    with open(f'./ip_check/ip_repo/{time_stamp}.txt', mode='a+') as f:  #
        f.write(f"{ip},")


def ip_stats(pattern, output, new_folder=''):
    # Example Patterns glob('dir/*[0-9].*') or ('dir/file?.txt')
    merged = ''
    for path in glob(pattern):
        basename = os.path.basename(path)
        try:
            with open(path, 'r') as f:
                print()
                merged += f.read()
            print(path, 'read')
        except EmptyDataError:
            print(f'empty data @ {path}')

        if new_folder != '':
            shutil.move(path,os.path.join(new_folder, basename))

    all_ips = pd.DataFrame(merged.split(','), columns=['ip'])
    freq = all_ips['ip'].value_counts()
    freq.to_csv(output, index=False)

    # a small text file for summary stats

    summary = f'**** Total calls {sum(freq)} ****\nUnique IPs {len(freq)}, \nMost used IPs:\n{freq.head(20)}'
    summary_path = f'{Path(path).parent}/summary.txt'
    with open(summary_path, 'w+') as f:
        f.write(summary)
    print(f'IP frequencies saved @ {output}.\n{summary}')


def ip_rotation_test(M=''):
    ip_list = []
    time_stamps = []
    for i in range(10):
        try:
            session = requests.session()
            random.seed(datetime.now())
            creds = str(random.randint(10000, 0x7fffffff)) + ":" + "foobar"  # Different credential everytime => different IP
            session.proxies = {'http': f'socks5h://{creds}@localhost:9150', 'https': f'socks5h://{creds}@localhost:9150'}
            ip = get_ip(session)
            ip_list += [ip]
            time_stamp = datetime.now().strftime('%d-%H-%M') #day of month/hour/minute
            time_stamps += [time_stamp]
            print(f'{time_stamp}\t{ip}')
            store_ip(ip)

        except Exception as e:
            print(e)
            continue

    ip_records = pd.DataFrame(zip(ip_list, time_stamps))
    ip_records.to_csv(f'./ip_check/ip_records{M}.csv', index=True)

def get_new_ip():
    with Controller.from_port(port = 9051) as controller:
        controller.authenticate(password="password")
        controller.signal(Signal.NEWNYM)
        time.sleep(controller.get_newnym_wait())


def NewTorSession(report_ip = True):
    session = requests.session()
    random.seed(datetime.now())
    creds = str(random.randint(10000,0x7fffffff)) + ":" + "foobar" #Different credential everytime => different IP
    session.proxies = {'http': f'socks5h://{creds}@localhost:9150', 'https': f'socks5h://{creds}@localhost:9150'}
    if report_ip:
        ip = get_ip(session)
    else:
        ip = ''
    return session, ip

def NewSession(report_ip = True):
    session = requests.session()
    if report_ip:
        ip = get_ip(session)
    else:
        ip = ''
    return session, ip

def BalancedTorSession(): # make sure under-used ip are used
    Max_IP_Reuse = 45 # here its per hour
    Tier1_IP_Reuse = 40
    Tier2_IP_Reuse = 43
    keep_session = False

    for i in range(40):

        if random.random() < .01:
            session, ip = NewSession()
            N = ip_reuse_count(ip)
            print(f'using local ip {ip} which is used {N} times in the last hour.')
        else:
            session, ip = NewTorSession()
            N = ip_reuse_count(ip)


        if N < Tier1_IP_Reuse:
            keep_session = True
            break
        elif N < Tier2_IP_Reuse:
            if random.random()>.6:
                break
        elif N < Max_IP_Reuse:
            if random.random()>.9:
                break
        else:
            pass
        print(f'ip {ip} used {N} times in the last hour. trying to get another ip...')
    return session, ip, keep_session


def TorRequest(URL, save_ip_at=False, balanced_ip_use=True, session=False):
    '''alternative request method'''
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US',
        'Connection': 'keep-alive',
        'Cookie': 'Cookie; Cookie',
        'DNT': '1',
        # 'Host': 'e2.kase.gov.lv',
        'Referer': 'https://www.cars.com/',
        # 'Upgrade-Insecure-Requests': '1',
        'User-Agent': UserAgent().random}
    keep_session = False
    if session == False:
        if balanced_ip_use:
            session, ip, keep_session = BalancedTorSession()
            store_ip(ip)
        else:
            session, ip = NewTorSession(report_ip=False)
    html = session.get(URL, headers=headers, timeout=60).text
    #get_new_ip() # no need to explicitly change IP.
    if not keep_session or random.random()<.1:
        session = False
    return html, session, keep_session

def RegularRequest(URL, session=False):
    '''backup request method if TOR is disabled'''
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US',
        'Connection': 'keep-alive',
        'Cookie': 'Cookie; Cookie',
        'DNT': '1',
        # 'Host': 'e2.kase.gov.lv',
        'Referer': 'https://www.cars.com/',
        # 'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Safari/605.1.15'
    }
    keep_session = False
    if session == False:
        session = requests.session()
    html = session.get(URL, headers=headers, timeout=60).text
    return html, session, keep_session


if __name__=='__main__':
    pool = Pool()
    Max_Threads = 100
    pool.map(ip_rotation_test, range(Max_Threads))
    pool.close()
    pool.join()
    pattern_merge(pattern='./ip_check/ip_records[0-9]*.csv', output='./ip_check/ip_records_merged.csv', move_to_folder='./ip_check/hist')
    #pattern_merge(pattern='./ip_check/ip_list[0-9]*.csv', output='./ip_check/ip_list_merged.csv', new_folder='./ip_check/hist')
    ip_list = pd.read_csv('./ip_check/ip_records_merged.csv')['0']
    counter = collections.Counter(ip_list)
    print(counter)
    counter_df = pd.DataFrame(counter.values(), index=counter.keys(), columns=['count']).sort_values('count',ascending=False)
    print(f'total requests: {len(ip_list)}, unique IPs: {len(counter)}')
    counter_df.to_csv(f'./ip_check/ip_frequencies.csv', index=True)


# # TEST
# URL = 'https://www.cars.com/for-sale/searchresults.action/?perPage=100&searchSource=GN_REFINEMENT&sort=relevance&page=13&prMn=10475&prMx=10499&zc=85281&rd=9999'
# RegularRequest(URL)
# session = requests.session()
# headers = {
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#     'Accept-Encoding': 'gzip, deflate, br',
#     'Accept-Language': 'en-US',
#     'Connection': 'keep-alive',
#     'Cookie': 'Cookie; Cookie',
#     'DNT': '1',
#     #'Host': 'e2.kase.gov.lv',
#     'Referer': 'https://www.cars.com/',
#     #'Upgrade-Insecure-Requests': '1',
#     'User-Agent': UserAgent().random
# }
# html = session.get(URL, headers=headers).text
# print(html)


# Connect() is an easy way of connecting to TOR. Example below.
# import sys
#
# from stem.connection import connect
#
# if __name__ == '__main__':
#   controller = connect()
#
#   if not controller:
#     sys.exit(1)  # unable to get a connection
#
#   print(f'Tor is running version {controller.get_version()}')
#   controller.close()




#
#
# You can't open a new controller once you've connected to Tor. Try opening a controller right at the top of your script. Then both the Tor connection and signaller use the same controller object.
#
# This seems to work with Python3:
#
# import time
#
# import socket
# import socks
#
# import requests
# from bs4 import BeautifulSoup
# from stem import Signal
# from stem.control import Controller
#
# controller = Controller.from_port(port=9051)
#
#
# def connectTor():
#     socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5 , "127.0.0.1", 9050, True)
#     socket.socket = socks.socksocket
#
#
# def renew_tor():
#     controller.authenticate(<INSERT YOUR PASSPHRASE HERE>)
#     controller.signal(Signal.NEWNYM)
#
#
# def show_my_ip():
#     url = "http://www.showmyip.gr/"
#     r = requests.Session()
#     page = r.get(url)
#     soup = BeautifulSoup(page.content, "lxml")
#     ip_address = soup.find("span",{"class":"ip_address"}).text.strip()
#     print(ip_address)
#
#
# for i in range(10):
#     renew_tor()
#     connectTor()
#     showmyip()



from openget.network.downloader import Downloader

downloader = Downloader(proxy_enable=True)

my_ip = downloader.download({"url": "https://httpbin.org/ip", "proxies": None}).json()["origin"]

print(my_ip)

for i in range(10):
    proxy_ip = downloader.download({"url": "https://httpbin.org/ip"}).json()["origin"]
    assert proxy_ip != my_ip
    print(f"{my_ip} != {proxy_ip}")

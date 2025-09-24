import requests
proxyip = "http://smart-scraper:naveed155@proxy.smartproxy.net:3120"
url = "https://api.ip.cc"
proxies={
    'http':proxyip,
    'https':proxyip,
}
data = requests.get(url=url,proxies=proxies)
print("data----------",data.text)

#coding=utf-8
import requests
import re

def get_videos():
     headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
     'Accept-Encoding':'gzip, deflate',
     'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
     'Cache-Control': 'max-age=0',
     'Connection': 'keep-alive',
     'Cookie': '__ysuid=1531120273497GeD; juid=01cj40nuo91c9f; cna=+PbJE0QaJE0CAaN98yxiNfHR; __aysid=1532773779576Euq; __ayft=1532845978522; __ayscnt=1; __arpvid=1532846057868gNQ03Q-1532846057880; __aypstp=3; __ayspstp=5; isg=BC4uc1na-URriA2qjJCdqGbcf4T66fM2J2onSFj3mzHsO86VwL9COdQ59-dy4-pB; __ayvstp=7; __aysvstp=14',
     'Host': 'player.youku.com',
     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}
     urls=['http://pl-ali.youku.com/playlist/m3u8?vid=XMzc0NjU1NDM4MA%3D%3D&type=flv&ups_client_netip=a37d158f&utid=%2BPbJE0QaJE0CAaN98yxiNfHR&ccode=0512&psid=2a52bf601b178253e829bc28044fad5d&duration=573&expire=18000&drm_type=1&drm_device=7&ups_ts=1532848420&onOff=0&encr=0&ups_key=80c011fefd524e73b92aaac8c8db4403',
           'http://pl-ali.youku.com/playlist/m3u8?vid=XMzY2MTQ3MjgwMA%3D%3D&type=flv&ups_client_netip=a37d1506&utid=%2BPbJE0QaJE0CAaN98yxiNfHR&ccode=0512&psid=0261b7232c46e03294adf707b40b2fde&duration=230&expire=18000&drm_type=1&drm_device=7&ups_ts=1532847748&onOff=0&encr=0&ups_key=0ae63f4baadb6c7752944f267478ad7a',
           'http://pl-ali.youku.com/playlist/m3u8?vid=XMzc0MjQ4Mzc4NA%3D%3D&type=flv&ups_client_netip=a37d158b&utid=%2BPbJE0QaJE0CAaN98yxiNfHR&ccode=0512&psid=0c85de2450c88817b3e8bbb0e2a5b30e&duration=531&expire=18000&drm_type=1&drm_device=7&ups_ts=1532848543&onOff=0&encr=0&ups_key=ce982295c20f9483d02fcfda97b6c427',
           'http://pl-ali.youku.com/playlist/m3u8?vid=XMzcyOTAzMzAyMA%3D%3D&type=flv&ups_client_netip=a37d1506&utid=%2BPbJE0QaJE0CAaN98yxiNfHR&ccode=0512&psid=3c66e5ddf50d324d879afdbbf4308a00&duration=309&expire=18000&drm_type=1&drm_device=7&ups_ts=1532848970&onOff=0&encr=0&ups_key=1f7e0f5c6c474f6ad3d9f5b5d0b1d40a']
     for url in urls:
          response=requests.get(url=url,headers=headers)
          filename=re.search('key=(.*)',url,re.S).group(1)
          filename='fish-video'+filename+'.mp4'
          content=response.content

          with open(filename,'wb') as f:
               f.write(response.content)


if __name__ == '__main__':

     get_videos()





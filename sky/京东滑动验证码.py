# encoding=utf8
import cv2

import time

from io import BytesIO

from PIL import Image

import numpy as np

from selenium import webdriver

from selenium.webdriver.common.action_chains import ActionChains

from selenium.webdriver.common.by import By

from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.support.ui import WebDriverWait



class Crack():

  def __init__(self):

    self.url='https://ssl.zc.qq.com/v3/index-chs.html?type=3'
    self.driver=webdriver.Chrome()
    self.wait=WebDriverWait(self.driver,20)
    self.zoom=1



  def save_pic(self,href, name):

      js = " window.open('{}')".format(href)

      self.driver.execute_script(js)

      current_handle =self.driver.current_window_handle

      for handle in self.driver.window_handles:

          if handle != current_handle:

              self.driver.switch_to_window(handle)

              self.driver.save_screenshot('{}'.format(name))

              element = self.driver.find_element_by_tag_name('img')

              print(element.size['width'])

              left = element.location['x']

              top = element.location['y']

              right = element.location['x'] + element.size['width']

              bottom = element.location['y'] + element.size['height']

              image = Image.open('{}'.format(name))

              image = image.crop((left, top, right, bottom))
              


              image.save('{}'.format(name))

      self.driver.close()

      self.driver.switch_to_window(current_handle)

  def open(self):
      self.driver.get(self.url)


  def get_pic(self):

      

      self.driver.switch_to.frame(self.wait.until(EC.presence_of_element_located((By.XPATH,"//iframe[@frameborder='0']"))))

      template_src =self.wait.until(EC.presence_of_element_located((By.ID,'slideBkg'))).get_attribute('src')

      target_src =self.wait.until(EC.presence_of_element_located((By.ID,'slideBlock'))).get_attribute('src')


      self.save_pic(target_src,'target.png')

      self.save_pic(template_src,'template.png')
      
      
      local_img=Image.open('template.png')

      size_loc=local_img.size

      print(size_loc[0])
      
      self.zoom=280/int(size_loc[0])
      




  def show(self,name):
    cv2.imshow('Show', name)
    cv2.waitKey(0)
    cv2.destroyAllWindows()




  def get_distance(self):
    otemp = 'template.png'
    oblk = 'target.png'
    template=cv2.imread(otemp,0)
     
    target=cv2.imread(oblk,0)

    target=abs(255-target)


    run=1
    w,h=target.shape[::-1]
    #print(w,h)
    result=cv2.matchTemplate(target,template,cv2.TM_CCOEFF_NORMED)
    #使用二分法查找阈值的精确值
    L=0
    R=1
    while run<20:
      run+=1
      threshold=(R+L)/2
      print(threshold)
      if threshold<0:
        print('Error')
        return None 
      loc=np.where(result>=threshold)
      print(len(loc[1]))
      if len(loc[1]) >1:
        L+=(R-L)/2
      elif len(loc[1]) ==1:
        print('目标区域起点X坐标为：%d'%loc[1][0])
        break
      elif len(loc[1])<1:
        R=R-(R-L)/2

    for pt in zip(*loc[::-1]): 
      cv2.rectangle(template, pt, (pt[0] + w, pt[1] + h), (7, 279, 151), 2) 
    cv2.imshow('Dectected', template) 
    cv2.waitKey(0) 
    cv2.destroyAllWindows() 
    return loc[1][0]



  def slide(self,distance):

    track =[]

    current =0

    mid=distance*0.6

    t=0.2 

    v=0 

    while current <distance:
      if current <mid :
        a=2 

      else:
        a=-3

      v0 =v 
      v=v0 +a*t 
      move =v0*t +1/2*a*t*t 

      current +=move 

      track.append(round(move))



    #track.append(distance-current)



    #滑动操作 
    self.driver.switch_to.frame(self.wait.until(EC.presence_of_element_located((By.XPATH,"//iframe[@frameborder='0']"))))
    slide=self.wait.until(EC.presence_of_element_located((By.XPATH,'.//div[@id="tcaptcha_drag_thumb"]')))

    action=ActionChains(self.driver)

    action.click_and_hold(slide).perform()
    

    

    for i in track:
      
      action.move_by_offset(xoffset=i, yoffset=0).perform()



      action =ActionChains(self.driver)

      print(i,slide.location['x'])


    time.sleep(1)
    action.release().perform()

  def main(self):
    self.open()
    
    self.get_pic()
    distance=int((self.get_distance()+25)*self.zoom)-22
    print('实际滑动的距离%s'%distance)
    self.slide(distance)
    time.sleep(10)
    self.driver.close()
    self.driver.quit()
      


if __name__=="__main__":
  crack=Crack()
  #crack.main()
  crack.get_distance()



  






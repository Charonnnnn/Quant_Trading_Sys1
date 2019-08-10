from fontTools.ttLib import TTFont


# font = TTFont('online.woff') # http://data.eastmoney.com/font/729/729cd4b38d7c4276ac9c2e51d384a029.woff
# font.saveXML('online.xml')
#
# font = TTFont('base.woff')   # vfile.meituan.net/colorstone/163fe109f89cf994a941b8b9ad3dff5b2084.woff
# font.saveXML('base.xml')

#=============

# font1 = TTFont("base.woff")
#
# uni_list1=font1.getGlyphOrder()[2:]     # 所有编码
#
# font_objs = []
# for unicode in uni_list1:
#     font_objs.append(font1['glyf'][unicode])
#
# # print(font_objs)
# uni_list = ["8","6","4","7","0","9","1","2","5","3"]
#
# uni_cmap = []
# for i in range(len(uni_list)):
#     uni_cmap.append((font_objs[i], uni_list[i])) # 将字符对象对应字符以元组的形式放在列表中
#
# print(uni_cmap)
#
# # -----------------------------------------------------------------------------------
# # 模拟第二次请求获得的字体数据
# font2=TTFont('online.woff')              # 打开访问网页新获得的字体文件02.ttf
#
# uni_list2=font2.getGlyphOrder()[2:]     # 所有编码信息
# print(uni_list2)
# # ['uniE903', 'uniF144', 'uniEEE8', 'uniF53A', 'uniEB55', 'uniEA24', 'uniF897', 'uniEE9B', 'uniE410', 'uniF25E']
#
# for uni2 in uni_list2:
#     obj2=font2['glyf'][uni2]  #获取编码uni2在font2.woff中对应的对象
#     for obj in uni_cmap:
#         if obj[0]==obj2:        # 比较两个字体文件的字形对象是否相同
#             print(uni2,obj[1])  # 打印结果，编码uni2和对应的数字

font1 = TTFont("3628e1717a90462fbff2a50ce0c7e046.woff")
uni_list2=font1.getGlyphOrder()[2:]     # 所有编码信息
print(uni_list2)
fontdict = {'zbxtdyc':4, 'whyhyx':9, 'wqqdzs':3, 'bgldyy':7, 'nhpdjl':5, 'qqdwzl':1, 'bdzypyc':0, 'zwdxtdy':8, 'sxyzdxn':6, 'zrwqqdl':2}


#==================================
# import os
# import time
# import re
# import requests
# from fontTools.ttLib import TTFont
# from fake_useragent import UserAgent
# from bs4 import BeautifulSoup
#
# host = 'http://maoyan.com'
#
#
# def main():
#     url = 'http://maoyan.com/films?yearId=13&offset=0'
#     get_moviescore(url)
#
#
# os.makedirs('font', exist_ok=True)
# regex_woff = re.compile("(?<=url\(').*\.woff(?='\))")
# regex_text = re.compile('(?<=<span class="stonefont">).*?(?=</span>)')
# regex_font = re.compile('(?<=&#x).{4}(?=;)')
#
# basefont = TTFont('base.woff')
# fontdict = {'uniF65D': '8', 'uniE991': '6', 'uniE544': '4', 'uniF582': '7', 'uniE133': '0',
#             'uniEE06': '9', 'uniECF3': '1', 'uniEBE2': '2', 'uniF4A3': '5', 'uniF063': '3'}
#
#
# def get_moviescore(url):
#     # headers = {"User-Agent": UserAgent(verify_ssl=False).random}
#     headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
#                              'Chrome/68.0.3440.106 Safari/537.36'}
#     html = requests.get(url, headers=headers).text
#     soup = BeautifulSoup(html, 'lxml')
#     ddlist = soup.find_all('dd')
#     for dd in ddlist:
#         a = dd.find('a')
#         if a is not None:
#             link = host + a['href']
#             time.sleep(5)
#             dhtml = requests.get(link, headers=headers).text
#             msg = {}
#
#             dsoup = BeautifulSoup(dhtml, 'lxml')
#             msg['name'] = dsoup.find(class_='name').text
#             ell = dsoup.find_all('li', {'class': 'ellipsis'})
#             msg['type'] = ell[0].text
#             msg['country'] = ell[1].text.split('/')[0].strip()
#             msg['length'] = ell[1].text.split('/')[1].strip()
#             msg['release-time'] = ell[2].text[:10]
#
#             # 下载字体文件
#             woff = regex_woff.search(dhtml).group()
#             wofflink = 'http:' + woff
#             localname = 'font\\' + os.path.basename(wofflink)
#             if not os.path.exists(localname):
#                 downloads(wofflink, localname)
#             font = TTFont(localname)
#
#             # 其中含有 unicode 字符，BeautifulSoup 无法正常显示，只能用原始文本通过正则获取
#             ms = regex_text.findall(dhtml)
#             if len(ms) < 3:
#                 msg['score'] = '0'
#                 msg['score-num'] = '0'
#                 msg['box-office'] = '0'
#             else:
#                 msg['score'] = get_fontnumber(font, ms[0])
#                 msg['score-num'] = get_fontnumber(font, ms[1])
#                 msg['box-office'] = get_fontnumber(font, ms[2]) + dsoup.find('span', class_='unit').text
#             print(msg)
#
#
# def get_fontnumber(newfont, text):
#     ms = regex_font.findall(text)
#     for m in ms:
#         text = text.replace(f'&#x{m};', get_num(newfont, f'uni{m.upper()}'))
#     return text
#
#
# def get_num(newfont, name):
#     uni = newfont['glyf'][name]
#     for k, v in fontdict.items():
#         if uni == basefont['glyf'][k]:
#             return v
#
#
# def downloads(url, localfn):
#     with open(localfn, 'wb+') as sw:
#         sw.write(requests.get(url).content)
#
#
# if __name__ == '__main__':
#     main()
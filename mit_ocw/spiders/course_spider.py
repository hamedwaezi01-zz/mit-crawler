import sys
import os
import logging
from time import time
import pandas as pd

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor
from scrapy import Selector

from scrapy import signals
from pydispatch.dispatcher import connect


crawling_id = int(time())//10
handler = logging.FileHandler(os.path.join('mit_ocw','logs',f'log_{crawling_id}.txt'))
handler.setFormatter(logging.Formatter('%(asctime)s - [%(name)s] - {%(levelname)s} - %(message)s'))
logger = logging.getLogger(__name__)
logging.getLogger().addHandler(handler)

'''
Data representation in the following csv file
	page_id:
		dept:
		title:
		professor:
		objective:
		prerequisites:
		refrences:
		scores:
		description:
		projects:
		university homepage:
		professor_page:
'''

data = pd.DataFrame(columns=[
	'page_id','dept','title', 'professors', 'description', 'prerequisites_skills', 
	'objectives_Outcome','refrences','scores','description', 'assignments', 'professor_page'
	])



class CourseSpider(scrapy.Spider):
	name = "course"
	allowed_domains = ['ocw.mit.edu']


	def __init__(self, *args, **kwargs):
		super(CourseSpider, self).__init__(*args, **kwargs)
		connect(self.quit, signals.spider_closed)

# It will be called when the spider stops crawling
	def quit(self, spider):
		pass
		


	def start_requests(self):
		urls = ['https://ocw.mit.edu/courses/find-by-department/']

		for url in urls:
			yield scrapy.Request(url=url, callback=self.first_parse)
	
	def first_parse(self,response):
		current_url = response.request.url.split('/')
		
		if len(current_url) == 6 and current_url[3]=="courses":
			topics = response.xpath("""//ul[@class="deptList"]/li/a/@href""").getall()
			for topic in topics:
				yield response.follow(topic,callback=self.parse_courses)

	def parse_courses(self, response):
		links = response.xpath("""//ul[@class="courseRow"]""").getall()
		for link in links:
			selector = Selector(text=link)
			print("***********************************************")
			print(selector.xpath('//li[@class="courseNumCol"]/atext()').extract()[0].strip())
			print(selector.xpath('//li[@class="courseNumCol"]/a@href').extract()[0].strip())
			print("##########################################################")
			yield response.follow(selector.xpath('//li[@class="courseNumCol"]/a/@href').extract()[0].strip(),
				callback=self.parse, cb_kwargs={
				'page_id':selector.xpath('//li[@class="courseNumCol"]/a/text()').extract()[0].strip(),
				# 'level':	selector.xpath('//li[@class="courseLevelCol"]/a/text()')
				})

	def parse(self, response, page_id=None, tought_in=None):
		# temp = response.xpath("""//*[@class="global"]/span[@id="parent-fieldname-title"]""").getall()

		# if len(temp) == 1:
			
		# else :
		temp = response.xpath("""//nav[@id="course_nav"]""").getall()
		if len(temp) == 1:
			current = response.xpath("""//nav[@id="course_nav"]/ul/li[@class="selected"]/a/text()""").extract()[0]
			if		current == "Course Home":
				profs = response.xpath("""//p[@class="ins"]/text()""").getall()
				# response.xpath("""//div[@id="course_info" and h3[text()="MIT Course Number"]]/""")
				# course_number = response.xpath("""//div[@id="course_info"]/h3[text()="MIT Course Number"]/following-sibling::p[1]/text()""")

				taught_in = response.xpath("""//div[@id="course_info"]/h3[text()="As Taught In"]/following-sibling::p[1]/text()""")
				level = response.xpath("""//div[@id="course_info"]/h3[text()="Level"]/following-sibling::p[1]/text()""")
				dept = response.xpath("""//nav[@id="breadcrumb_chp"]/p/a[3]/text()""")
				title = response.xpath("""//div[@id="course_title"]/h1/text()""")
				sections_links = response.xpath("""//nav[@id="course_nav"]/ul/li/a/@href """).getall()
				
				_id = page_id + ("" if not taught_in else ("_"+taught_in))
				data[_id] = {
					"dept": dept,
					"title": title,
					"professors": profs,
					'level': level,
					"taught_in": taught_in,
					# "objective": 
					# "prerequisites":
					# "refrences":
					# "scores":
					# "description":
					# "projects":
					# "professor_page":
				}
				for links in sections_links:
					yield response.follow(link, callback=self.parse, cb_kwargs={
					'page_id':_id,
					})
			elif	current == "Syllabus":
				times = response.xpath("""//main[@id="course_inner_section"]/h2[text()="Course Meeting Times"]/following-sibling::*[(following-sibling::h2[preceding-sibling::h2[1][text()="Course Meeting Times"]]) | (following-sibling::div[@class="help slide-bottom" and preceding-sibling::h2[1][text()="Course Meeting Times"]])]/text() """).getall()
				desc_text = """ text()="Course Description" or text()="Description" """
				desc = response.xpath(
					# f"""//main[@id="course_inner_section"]/*[(following-sibling::h2[preceding-sibling::h2[1][({desc_text})]]) | (following-sibling::div[@class="help slide-bottom" and preceding-sibling::h2[1][text()={desc_text}]])]/text()"""
					f"""//main[@id="course_inner_section"]/*[(following-sibling::h2[1][preceding-sibling::h2[1][contains(., "Description")]]) | (following-sibling::div[@class="help slide-bottom" and preceding-sibling::h2[1][contains(.,'Description')]])]/text()"""
					).getall()
				# obj_text = '''text()="Course Learning Objectives" or text()="Course Objectives" or text()="Course Objectives and Outcomes" '''
				objectives = response.xpath(
					# f"""//*[@id="course_inner_section"]/h2[@class="subhead" and ({obj_text})]/following-sibling::*[(following-sibling::h2[preceding-sibling::h2[1][{obj_text}]]) or (following-sibling::div[@class="help slide-bottom" and preceding-sibling::h2[1][text()={obj_text}]]))]/text()"""
					f"""//main[@id="course_inner_section"]/h2[@class="subhead" and contains(.,"Objective")]/following-sibling::*[(following-sibling::h2[preceding-sibling::h2[1][@class="subhead" and contains(.,"Objective")]]) | (following-sibling::div[@class="help slide-bottom" and preceding-sibling::h2[1][contains(.,"Objective")]])]/text()"""
					# '''//main[@id="course_inner_section"]/h2[@class="subhead" and contains(.,"Objective")]'''
				).getall()
				
				prereq_text = """ text()="Prerequisites" """
				prerequisites = response.xpath("""
					//*[@id="course_inner_section"]/h2[@class="subhead" and text()="Prerequisites" ]/following-sibling::*[(following-sibling::h2[preceding-sibling::h2[1][text()="Prerequisites"]]) or (following-sibling::div[@class="help slide-bottom" and preceding-sibling::h2[1][text()="Prerequisites"]])]
				""")
				# data[page_id] = 
			elif	current == "Calendar":
				pass
			elif	current == "Assignments":
				pass
			elif	current == "Projects":
				pass
			links = response.xpath("""//nav[@id="course_nav"]/ul/li/a/@href""").getall()
			for link in links:
				yield response.follow(link, callback=self.parse)
"""
//*[@id="course_inner_section"]/h2[@class="subhead" and (text()="Course Learning Objectives" or text()="Course Objectives" or text()="Course Objectives and Outcomes" )]/following-sibling::*[(following-sibling::h2[preceding-sibling::h2[1][text()="Course Learning Objectives" or text()="Course Objectives" or text()="Course Objectives and Outcomes" ]]) or ((following-sibling::div[@class="help slide-bottom" and preceding-sibling::h2[1][text()=text()="Course Learning Objectives" or text()="Course Objectives" or text()="Course Objectives and Outcomes" ]]))]/text()
"""
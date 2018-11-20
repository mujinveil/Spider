import re

import gevent.monkey
from bello_jtl import Interpreter
from gevent.pool import Pool
from lxml import etree
from redis import StrictRedis, ConnectionPool

from utils import cos_client, Mongo, MONGO_URL, SHlogger, parse_json
from utils.tools import format_time
from utils.transform_map import RESUME_TRANSFORM_MAP

logger = SHlogger().logger
db = Mongo(MONGO_URL, db_name="spider", coll_name='dajie_resume', unique_index='external_id')
collection = Mongo(MONGO_URL, db_name='temp', coll_name='spider_resume_test')
redis_conn = StrictRedis(connection_pool=ConnectionPool(host='10.0.40.16', port=6379, db=7, decode_responses=True))


class Parser(object):
    def __init__(self):
        pass

    def get_id(self):
        # docs = db.find({'_options.update_count': {'$ne': 1}}).limit(50000)
        docs = db.find({'_options.update_count': 1}).limit(200000)
        for doc in docs:
            url = doc['from_url']
            if 'apply/uncomplete' in url:
                external_id = doc['external_id']
                logger.info(external_id)
                redis_conn.rpush('dajie_parse', external_id)

    def extract(self, html):

        '''
        从html里面抽取字段
        '''
        content = etree.HTML(html)

        name = ''.join(
            content.xpath(".//h5[contains(text(),'先生')]/text() | //h5[contains(text(),'女士')]/text()")).strip()
        try:
            surname = name[0]
        except:
            surname = None

        tag = ''.join(
            content.xpath(".//span[contains(text(),'男')]/text() | //span[contains(text(),'女')]/text()")).strip()
        try:
            sex = re.search('男|女', tag, re.S).group()
        except:
            sex = None

        try:
            age = re.search('\d+', tag, re.S).group()
        except:
            age = None
        try:
            last_company = ''.join(content.xpath(".//li[contains(text(),'所在公司')]/text()")).split('：')[1].strip()
        except:
            last_company = None
        try:
            last_job_title = \
                ''.join(content.xpath(".//li[contains(text(),'职') and contains(text(),'位')]/text()")).split('：')[
                    1].strip()
        except:
            last_job_title = None
        try:
            workedYearsMeanly = ''.join(content.xpath(".//li[contains(text(),'工作经验')]/text()")).split('：')[
                1].strip().replace('年', '').strip()
        except:
            workedYearsMeanly = None
        try:
            degree = ''.join(content.xpath(".//li[contains(text(),'学') and contains(text(),'历')]/text()")).split('：')[
                1].strip()
        except:
            degree = None

        nowplace = ''.join(content.xpath(".//li[contains(text(),'所在城市')]/text()")).split('：')[1].strip()
        try:
            homeplace = \
                ''.join(content.xpath(".//li[contains(text(),'户') and contains(text(),'口')]/text()")).split('：')[
                    1].strip()
        except:
            homeplace = None

        try:
            politics_stat_cnt = ''.join(content.xpath(".//li[contains(text(),'政治面貌')]/text()")).split('：')[
                1].strip()
        except:
            politics_stat_cnt = None

        try:
            birthday = ''.join(content.xpath(".//li[contains(text(),'出生日期')]/text()")).split('：')[1].strip()
        except:
            birthday = None

        # 求职状态,期望地点,期望职位,工作性质,期望行业,期望薪资,到岗时间
        job_status = ''.join(content.xpath(".//th[contains(text(),'目前状态')]/../td/p/text()")).strip()

        workType = ''.join(content.xpath(".//th[contains(text(),'求职类型')]/../td/p/text()")).strip()

        job_position = ''.join(content.xpath(".//th[contains(text(),'期望职业')]/../td/p/text()")).strip()

        expect_job = ''.join(content.xpath(".//th[contains(text(),'期望行业')]/../td/p/text()")).strip()

        expect_local = ''.join(content.xpath(".//th[contains(text(),'期望城市')]/../td/p/text()")).strip()

        expect_salary = ''.join(content.xpath(".//th[contains(text(),'期望月薪')]/../td/p/text()")).strip()

        resume_update_time = format_time(
            content.xpath('//p[contains(text(),"更新时间")]/text()')[0].replace('更新时间：', '')).strftime("%Y-%m-%d")

        basics = {
            'name': name,
            'surname': surname,
            'gender': sex,
            'birthday': birthday,
            'age': age,
            'hukou': homeplace,
            'politics_status': politics_stat_cnt,
            'top_edu_degree': degree,
            'current_location': nowplace,
            'year_of_work_experience': workedYearsMeanly,
            'last_job_title': last_job_title,
            'last_company': last_company,
            'current_status': job_status,
            'expected_job_title': job_position,
            'expected_industry': expect_job,
            'expected_job_nature': workType,
            'expected_locations': expect_local,
            'expected_salary': expect_salary,

        }

        '''
        教育经历
        '''
        educate_exp = content.xpath("//span[contains(text(),'教育经历')]/../../dd//tr")

        educations = []

        for e in educate_exp:
            try:
                time = ''.join(e.xpath('.//th/text()[1]'))

                start_time = format_time(str(time.split('至')[0])).strftime("%Y-%m")

                end_time = str(time.split('至')[1]).strip()

                if end_time == "今":

                    end_time = resume_update_time

                else:
                    end_time = format_time(end_time).strftime("%Y-%m")

                school_name = ''.join(e.xpath('.//td/p[@class="item-tit highlight-filter"]/span/text()'))

                degree = \
                    ''.join(e.xpath('.//td/p[@class="item-tit highlight-filter"]/text()')).replace(school_name,
                                                                                                   '').split(
                        ' ')[0]

                professional = \
                    ''.join(e.xpath('.//td/p[@class="item-tit highlight-filter"]/text()')).replace(school_name,
                                                                                                   '').split(
                        ' ')[1]

                description = ''.join(e.xpath('.//td/p[@class="item-des highlight-filter"]/text()')).strip()

                edu = {
                    'school_name': school_name,
                    'start_date': start_time,
                    'end_date': end_time,
                    'major': professional,
                    'degree': degree,
                    'description': description
                }

                educations.append(edu)
            except:
                pass

        '''
        工作经历
        '''
        work_exp = content.xpath("//span[contains(text(),'工作经历')]/../../dd//tr")

        employments = []

        for work in work_exp:
            try:

                time = ''.join(work.xpath('.//th/text()[1]'))

                start_time = format_time(str(time.split('至')[0])).strftime("%Y-%m")

                end_time = str(time.split('至')[1]).strip()

                if end_time == "今":
                    end_time = resume_update_time

                else:
                    end_time = format_time(end_time).strftime("%Y-%m")

                job_name = ''.join(work.xpath('.//td/p[1]/span/text()'))

                company_name = job_name.split('（')[0].strip()

                industry = job_name.split('（')[1].replace('）', '').strip()

                title = ''.join(work.xpath('.//td/p[1]/text()')).replace(job_name, '')

                total = ''.join(work.xpath('.//td/p[2]/text()')).split('\n')

                company_type = company_size_cnt = department = report_to = substaff_number = salary = None

                for i in total:

                    if '公司性质' in i:
                        company_type = ''.join(i.split('：')[1]).strip()
                    if '公司规模' in i:
                        company_size_cnt = ''.join(i.split('：')[1]).strip()
                    if '所在部门' in i:
                        department = ''.join(i.split('：')[1]).strip()
                    if '汇报对象' in i:
                        report_to = ''.join(i.split('：')[1]).strip()
                    if '下属人数' in i:
                        substaff_number = ''.join(i.split('：')[1]).strip()
                    if '薪水' in i:
                        salary = ''.join(i.split('：')[1]).strip()

                describe = ''.join(work.xpath('.//td/p[3]/text()')).strip()

                job = {
                    'company_name': company_name,
                    'start_date': start_time,
                    'end_date': end_time,
                    'salary': salary,
                    'department': department,
                    'industry': industry,
                    'company_nature': company_type,
                    'company_scale': company_size_cnt,
                    'description': describe,
                    'title': title,
                    'report_to': report_to,
                    'substaff_number': substaff_number
                }
                employments.append(job)
            except:
                pass

        '''
        项目经验
       
        '''
        project_exp = content.xpath("//span[contains(text(),'项目经验')]/../../dd//tr")

        projects = []

        for p in project_exp:
            try:
                time = ''.join(p.xpath('.//th/text()[1]'))

                start_time = format_time(str(time.split('至')[0])).strftime("%Y-%m")

                end_time = str(time.split('至')[1]).strip()

                if end_time == "今":
                    end_time = resume_update_time

                else:
                    end_time = format_time(end_time).strftime("%Y-%m")

                role = ''.join(p.xpath('.//td/p[1]/span/text()'))

                project_name = ''.join(p.xpath('.//td/p[1]/text()')).replace(role, '')

                describe = ''.join(p.xpath('.//td/p[3]/text()')).strip()

                achievements = ''.join(p.xpath('.//td/p[4]/text()')).strip()

                project = {'project_name': project_name,
                           'start_date': start_time,
                           'end_date': end_time,
                           'achievements': achievements,
                           'role': role,
                           'description': describe
                           }

                projects.append(project)
            except:
                pass

        '''
        实习经历
        '''

        second_work = content.xpath("//span[contains(text(),'实习经历')]/../../dd//tr")

        secondary_employments = []

        for work in second_work:
            try:
                time = ''.join(work.xpath('.//th/text()[1]'))

                start_time = format_time(str(time.split('至')[0])).strftime("%Y-%m")

                end_time = str(time.split('至')[1]).strip()

                if end_time == "今":
                    end_time = resume_update_time

                else:
                    end_time = format_time(end_time).strftime("%Y-%m")

                job_name = ''.join(work.xpath('.//td/p[1]/span/text()'))

                company_name = job_name.split('（')[0].strip()

                industry = job_name.split('（')[1].replace('）', '').strip()

                title = ''.join(work.xpath('.//td/p[1]/text()')).replace(job_name, '')

                job = {
                    'company_name': company_name,
                    'start_date': start_time,
                    'end_date': end_time,
                    'industry': industry,
                    'title': title,

                }

                secondary_employments.append(job)
            except:
                pass

        '''
        自我描述
        '''
        summary = ''.join(content.xpath(".//span[contains(text(),'自我介绍')]/../../dd//tr//p/text()")).strip()

        '''
        他的专长 it 技能
        '''
        skill_items = content.xpath(".//span[contains(text(),'他的专长')]/../..//tbody//td/p/span")

        skills = []

        for s in skill_items:
            skill_name = ''.join(s.xpath('.//text()')).strip()

            skill = {'skill_name': skill_name}

            skills.append(skill)

        it_skills = content.xpath(".//span[contains(text(),'IT技能')]/../../dd//tr/td")

        for s in it_skills:

            if '精通' in ''.join(s.xpath(".//p/text()")):
                for jt in s.xpath(".//p/span"):
                    skill_name = ''.join(jt.xpath(".//text()")).strip()
                    skill_level = '精通'
                    skill = {'skill_name': skill_name,
                             'skill_level': skill_level}
                    skills.append(skill)

            elif '熟悉' in ''.join(s.xpath(".//p/text()")):
                for sx in s.xpath(".//p/span"):
                    skill_name = ''.join(sx.xpath(".//text()")).strip()
                    skill_level = '熟悉'
                    skill = {'skill_name': skill_name,
                             'skill_level': skill_level}
                    skills.append(skill)

        '''
        语言能力
        '''
        language_exp = content.xpath("//span[contains(text(),'语言能力')]/../../dd//tr/td")

        languages = []

        for l in language_exp:
            try:
                l_name = ''.join(l.xpath(".//p[@class='item-tit']/text()"))
                r_w = ''.join(l.xpath(".//p/span[contains(text(),'读写')]/text()")).split('：')[1]
                l_s = ''.join(l.xpath(".//p/span[contains(text(),'听说')]/text()")).split('：')[1]
                cert_level = ''.join(l.xpath(".//p/span[3]/text()")).strip().split('：')[0]
                language = {'language': l_name,
                            'read_and_write': r_w,
                            'listen_and_speak': l_s,
                            'cert_level': cert_level}
                languages.append(language)
            except:
                pass

        '''
        培训经历
        '''
        training_exp = content.xpath("//span[contains(text(),'培训经历')]/../../dd//tr")

        trainings = []

        for tr in training_exp:
            try:
                time = ''.join(tr.xpath('.//th/text()[1]'))

                start_time = format_time(str(time.split('至')[0])).strftime("%Y-%m")

                end_time = str(time.split('至')[1]).strip()

                if end_time == "今":
                    end_time = resume_update_time

                else:
                    end_time = format_time(end_time).strftime("%Y-%m")

                org = ''.join(tr.xpath('.//td/p[@class="item-tit highlight-filter"]/span/text()'))

                name = ''.join(tr.xpath('.//td/p[@class="item-tit highlight-filter"]/text()')).replace(org, '')

                training_content = ''.join(tr.xpath('.//td/p[2]/text()')).strip()

                training = {
                    'name': name,
                    'start_date': start_time,
                    'end_date': end_time,
                    'org': org,
                    'traning_content': training_content,

                }

                trainings.append(training)
            except:
                pass

        '''
        证书
        '''

        certificate = content.xpath("//span[contains(text(),'证书')]/../../dd//tr/td/p/span")
        certificates = []
        for i in certificate:
            try:
                c = ''.join(i.xpath('.//text()')).strip()
                certificates.append(c)
            except:
                pass

        certifical = '\t'.join(certificates)
        if len(certifical) == 0:
            certifical = None

        '''
        其他
        '''

        sections = {
            # 'basic': '',                # 基本信息
            # 'campus_experience': '',    # 校园经历/社团活动
            'certificates': certifical,  # 所获证书
            # 'educations': '',           # 教育背景
            # 'courses': '',              # 所学课程
            # 'employments': '',          # 工作经历
            # 'expected_job_info': '',    # 期望工作信息
            # 'projects': '',             # 项目经历
            # 'social_experience': '',    # 社会活动/校外实践
            # 'skill_summary': '',        # 技能特长
            # 'awards': '',               # 荣誉成果
            'summary': summary,  # 个人介绍/自我描述/个人总结
            # 'hobbies': '',              # 兴趣爱好
            # 'publications': '',         # 论文发表等
            # 'languages': '',            # 语言能力
            # 'extra': '',                # 附加信息
        }

        '''
        汇总
        '''
        result = {
            'basics': basics,
            'educations': educations,
            'employments': employments,
            'secondary_employments': secondary_employments,
            'skills': skills,
            'projects': projects,
            'languages': languages,
            'sections': sections

        }

        return result

    def to_parse(self, external_id):
        doc = db.find_one({'external_id': external_id})

        key = doc['path']['id']
        try:
            data = cos_client.get_object(Bucket='jobs', Key=key)

        except:
            logger.warning('No such key')
            db.coll.delete_one({'external_id': external_id})
            logger.info('{} has been deleted'.format(external_id))

            return

        page = data['Body'].get_raw_stream().data.decode()

        content = etree.HTML(page)
        # logger.info('parse external_id:{}'.format(external_id))
        result = self.extract(page)

        if result:
            parser_result = parse_json(result)

            if not parser_result or parser_result.get('error'):
                logger.warning('parse_failed:{}'.format(external_id))

            else:

                result = Interpreter.transformJson(parser_result, RESUME_TRANSFORM_MAP)
                resume_update_time = format_time(
                    content.xpath('//p[contains(text(),"更新时间")]/text()')[0].replace('更新时间：', ''))
                result['resume_create_time'] = None
                result['resume_update_time'] = resume_update_time
                result['_created'] = doc['_created']
                result['path'] = doc['path']
                result['external_id'] = doc['external_id']
                result['channel'] = 'system.dajie'
                result['_search_options'] = doc['_search_options']
                result['from_url'] = doc['from_url']
                try:
                    result['_updated'] = doc['_updated']
                except:

                    result['_updated'] = None

                collection.replace_one({'external_id': external_id}, result, True)
                logger.info('parse_success:{}'.format(external_id))
                # db.update_one({'external_id': external_id}, {'$set': {'_options.update_count': 1}})

    def run(self):
        while True:
            try:
                external_id = redis_conn.lpop('dajie_parse')
            except:
                logger.warning('redis is empty')
                break
            else:
                if external_id is None:
                    return
                self.to_parse(external_id)


if __name__ == "__main__":
    gevent.monkey.patch_all()
    parse = Parser()
    # parse.to_parse('dajie_92b58bc9f90ecf145c871233c75a7391')

    # parse.get_id()
    # parse.run()

    p = Pool(50)
    for i in range(50):
        p.apply_async(parse.run)
    p.join()

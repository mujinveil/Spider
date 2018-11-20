from bello_jtl import Interpreter
from lxml import etree
from redis import StrictRedis, ConnectionPool
import gevent.monkey
from gevent.pool import Pool

from utils import cos_client, Mongo, MONGO_URL, SHlogger, parse_json
from utils.tools import format_time
from utils.transform_map import RESUME_TRANSFORM_MAP

logger = SHlogger().logger
db = Mongo(MONGO_URL, db_name="spider", coll_name='zhaopingou_resume', unique_index='external_id')
collection = Mongo(MONGO_URL, db_name='spider', coll_name='spider_resume_test')
redis_conn = StrictRedis(connection_pool=ConnectionPool(host='10.0.40.16', port=6379, db=7, decode_responses=True))


class Parser(object):
    def __init__(self):

        pass

    def get_id(self):
        docs = db.find({'_options.update_count': {'$ne': 1}}).limit(1000000)
        for doc in docs:
            #if '_updated' not in doc.keys():
                external_id = doc['external_id']
                logger.info(external_id)
                redis_conn.rpush('zhaopingou_parse', external_id)

    def extract(self, html):
        '''

        :param html:从html里面抽取字段
        :return:
        '''
        try:

            content = etree.HTML(html)
            # 性别，婚姻状态，年龄，生日，学历，国籍，政治面貌  婚姻状态不一定填
            name = content.xpath('.//div[@class="resumeb-head-top"]/h2/text()')[0]
            surname = name[0]
            base_info = content.xpath('.//div[@class="resumeb-head-con"]/ul/li/span/text()')
            sex = str(base_info[0])
            if len(base_info) == 7:
                isMarried = str(base_info[1])
                age = str(base_info[2]).replace('岁', '')
                birthday = format_time(str(base_info[3])).strftime("%Y-%m")
                degree = str(base_info[4])
                nationality = str(base_info[5]).split('：')[1]
                politics_stat_cnt = str(base_info[6])
            if len(base_info) == 6:
                if '未婚' in base_info or '已婚' in base_info:
                    isMarried = base_info[1]
                    age = str(base_info[2]).replace('岁', '')
                    birthday = None
                    degree = str(base_info[3])
                    nationality = str(base_info[4]).split('：')[1]
                    politics_stat_cnt = str(base_info[5])
                else:
                    isMarried = None
                    age = str(base_info[1]).replace('岁', '')
                    birthday = format_time(str(base_info[2])).strftime("%Y-%m")
                    degree = str(base_info[3])
                    nationality = str(base_info[4]).split('：')[1]
                    politics_stat_cnt = str(base_info[5])
            if len(base_info) == 5:
                isMarried = None
                age = str(base_info[1]).replace('岁', '')
                birthday = None
                degree = str(base_info[2])
                nationality = str(base_info[3]).split('：')[1]
                politics_stat_cnt = str(base_info[4])

            # 工作经验，户籍地，现居住地
            try:
                workedYearsMeanly = int(''.join(content.xpath('.//label[contains(text(),"工作经验")]/../text()')).replace("年工作经验",''))
            except:
                workedYearsMeanly = None
            try:
                homeplace = ''.join(content.xpath('.//label[contains(text(),"户") and contains(text(),"籍")]/../text()'))
            except:
                homeplace =None
            try:
                nowplace = ''.join(content.xpath('.//label[contains(text(),"现居住地")]/../text()'))
            except:
                nowplace=None

            # 求职状态,期望地点,期望职位,工作性质,期望行业,期望薪资,到岗时间

            job_status = ''.join(content.xpath('.//label[contains(text(),"求职状态")]/following-sibling::span[1]/text()'))
            if '离职' in job_status:
                job_status = '离职'
            elif '在职' in job_status:
                job_status = '在职'
            else:
                job_status =None

            expect_local = ''.join(content.xpath('.//label[contains(text(),"期望地点")]/following-sibling::span[1]/text()'))

            job_position = ''.join(content.xpath('.//label[contains(text(),"期望职位")]/following-sibling::span[1]/text()'))

            workType = ''.join(content.xpath('.//label[contains(text(),"工作性质")]/following-sibling::span[1]/text()'))

            expect_job = ''.join(content.xpath('.//label[contains(text(),"期望行业")]/following-sibling::span[1]/text()'))

            expect_salary = ''.join(
                content.xpath('.//label[contains(text(),"期望薪资")]/following-sibling::span[1]/text()'))

            arrivalTime = ''.join(content.xpath('.//label[contains(text(),"到岗时间")]/following-sibling::span[1]/text()'))

            resume_update_time = format_time(
                content.xpath('//span[contains(text(),"更新时间")]/text()')[0].replace('更新时间：', '')).strftime("%Y-%m-%d")

            basics = {
                'name': name,
                'surname': surname,
                'gender': sex,
                'birthday': birthday,
                'age': age,
                'marital_status': isMarried,
                'hukou': homeplace,
                'politics_status': politics_stat_cnt,
                'nationality': nationality,
                'top_edu_degree': degree,
                'current_location': nowplace,
                'year_of_work_experience': workedYearsMeanly,
                'current_status': job_status,
                'expected_job_title': job_position,
                'expected_industry': expect_job,
                'expected_job_nature': workType,
                'expected_locations': expect_local,
                'expected_onboard_time': arrivalTime,
                'expected_salary': expect_salary,

            }

            '''
            教育经历
            '''

            educate_exp = content.xpath('.//p[contains(text(),"教育经历")]/ancestor::dl/dd//div[@class="experience"]')

            educations = []

            for e in educate_exp:
                try:
                    time = ''.join(e.xpath('.//span[1]/text()'))

                    start_time = format_time(str(time.split('--')[0])).strftime("%Y-%m")

                    end_time = time.split('--')[1]
                    if end_time == '至今':

                        end_time = resume_update_time
                    else:
                        end_time = format_time(end_time).strftime("%Y-%m")

                    school_name = ''.join(e.xpath('.//span[2]/text()')).strip()

                    professional = ''.join(e.xpath('.//span[3]/text()')).strip()

                    degree = ''.join(e.xpath('.//span[4]/text()')).strip()

                    edu = {
                        'school_name': school_name,
                        'start_date': start_time,
                        'end_date': end_time,
                        'major': professional,
                        'degree': degree
                    }
                    educations.append(edu)
                except:
                    pass

            '''
            工作经历
            '''
            work_exp = content.xpath('.//p[contains(text(),"工作经历")]/ancestor::dl/dd//div[@class="workjl-list"]')

            employments = []

            for work in work_exp:
                time = ''.join(work.xpath('.//span[@class="dt"]/text()'))

                start_time = format_time(str(time.split('--')[0])).strftime("%Y-%m")

                end_time = time.split('--')[1]

                if end_time == '至今':
                    end_time = resume_update_time
                else:
                    end_time = format_time(end_time).strftime("%Y-%m")
                # 公司名称，职位，薪水，部门，行业类别，企业性质，公司规模，工作内容 ,其中薪水，部门，企业性质，公司规模不一定有
                job_name = ''.join(work.xpath('.//span[@class="dt"]/following-sibling::span[1]/text()')).strip()

                type = ''.join(work.xpath('.//div[@class="experience"][1]/text()')).split('|')[0].strip()
                try:
                    salary = ''.join(work.xpath('.//div[@class="experience"][1]/text()')).split('|')[1].strip()
                except:
                    salary = None
                try:
                    department = ''.join(work.xpath('.//div[@class="experience"][1]/text()')).split('|')[2].strip()
                except:
                    department = None

                try:
                    industry = ''.join(work.xpath('.//div[@class="experience"][2]/text()')).split('|')[0].split('：')[
                        1].strip()
                except:
                    industry = None

                try:
                    company_type = \
                        ''.join(work.xpath('.//div[@class="experience"][2]/text()')).split('|')[1].split('：')[1].strip()

                except:
                    company_type = None

                try:
                    company_size_cnt = \
                        ''.join(work.xpath('.//div[@class="experience"][2]/text()')).split('|')[2].split('：')[1].strip()
                except:
                    company_size_cnt = None
                describe = ''.join(work.xpath('.//div[@class="resumejl-text"]/text()')).strip()

                job = {
                    'company_name': job_name,
                    'start_date': start_time,
                    'end_date': end_time,
                    'salary': salary,
                    'department': department,
                    'industry': industry,
                    'company_nature': company_type,
                    'company_scale': company_size_cnt,
                    'description': describe,
                    'title': type
                }
                employments.append(job)

            '''
            项目经验
            '''
            project_exp = content.xpath('.//p[contains(text(),"项目经验")]/ancestor::dl/dd//div[@class="workjl-list"]')

            projects = []

            for p in project_exp:
                try:
                    time = ''.join(p.xpath('.//span[@class="dt"]/text()'))

                    start_time = format_time(str(time.split('--')[0])).strftime("%Y-%m")

                    end_time = time.split('--')[1]

                    if end_time == '至今':
                        end_time = resume_update_time
                    else:
                        end_time = format_time(end_time).strftime("%Y-%m")

                    project_name = ''.join(p.xpath('.//span[@class="dt"]/following-sibling::span[1]/text()')).split()[0].replace(
                        '项目名称:',
                        '').strip()
                    try:
                        role= ''.join(p.xpath('.//span[@class="dt"]/following-sibling::span[1]/text()')).split()[1]
                    except:
                        role=None
                    duty = ''.join(p.xpath('.//div[contains(text(),"项目职责")]/following-sibling::div[1]/text()')).strip()
                    describe = ''.join(
                        p.xpath('.//div[contains(text(),"项目描述")]/following-sibling::div[1]/text()')).strip()

                    project = {'project_name': project_name,
                               'start_date': start_time,
                               'end_date': end_time,
                               'responsibility': duty,
                               'role':role,
                               'description': describe
                               }
                    projects.append(project)
                except:
                    pass

            '''
            自我评价
            '''
            summary = ''.join(content.xpath('//p[contains(text(),"自我评价")]/ancestor::dl/dd/p/text()')).strip()

            '''
            语言能力
            '''
            language_exp = content.xpath('.//p[contains(text(),"语言能力")]/ancestor::dl/dd/p')
            languages = []
            for l in language_exp:
                try:
                    l_name = ''.join(l.xpath('.//text()')).split(' ')[0].strip()
                    r_w = ''.join(l.xpath('.//text()')).split(' ')[1].strip()
                    l_s = ''.join(l.xpath('.//text()')).split('|')[1].strip()
                    language = {'language': l_name, 'listen_and_speak': l_s, 'read_and_write': r_w}
                    languages.append(language)
                except:
                    pass

            '''
            培训经历
            '''
            train_exp = content.xpath('.//p[contains(text(),"培训经历")]/ancestor::dl/dd//div[@class="workjl-list"]')
            trainings = []
            for tr in train_exp:
                try:
                    time = ''.join(tr.xpath('.//span[@class="dt"]/text()'))

                    start_time = format_time(str(time.split('--')[0])).strftime("%Y-%m")

                    end_time = time.split('--')[1]

                    if end_time == '至今':
                        end_time = resume_update_time
                    else:
                        end_time = format_time(end_time).strftime("%Y-%m")

                    agency = ''.join(tr.xpath('.//span[@class="dt"]/following-sibling::span[1]/text()')).strip()

                    course = ''.join(
                        tr.xpath('.//div[contains(text(),"培训课程")]/following-sibling::div[1]/text()')).strip()

                    description = ''.join(
                        tr.xpath('.//div[contains(text(),"详细描述")]/following-sibling::div[1]/text()')).strip()

                    training = {'start_date': start_time,
                                'end_date': end_time,
                                'org': agency,
                                'name': course,
                                'training_content': description
                                }
                    trainings.append(training)
                except:
                    pass

            '''
            证书
            '''

            certificate = content.xpath('.//div[contains(text(),"获得证书")]/ancestor::div[@class="resumejl-content"]')
            certificates = []
            for i in certificate:
                try:
                    c = ''.join(i.xpath('.//div[@class="experience"]/text()')).split('|')[1].strip()
                    certificates.append(c)
                except Exception as e:
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

            result = {
                'basics': basics,
                'educations': educations,
                'employments': employments,
                'projects': projects,
                'languages': languages,
                'sections': sections

            }

            return result
        except Exception as e:
            logger.debug(e)

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
        result = self.extract(page)

        if result:
            parser_result = parse_json(result)

            if not parser_result or parser_result.get('error'):
                logger.warning('parse_failed:{}'.format(external_id))
                # redis_conn.rpush('zhaopingou_parse', external_id)
            else:

                result = Interpreter.transformJson(parser_result, RESUME_TRANSFORM_MAP)
                resume_update_time = format_time(
                    content.xpath('//span[contains(text(),"更新时间")]/text()')[0].replace('更新时间：', ''))
                result['resume_create_time'] = None
                result['resume_update_time'] = resume_update_time
                result['_created'] = doc['_created']
                result['path'] = doc['path']
                result['external_id'] = doc['external_id']
                result['channel'] = 'system.zhaopingou'
                result['_search_options'] = doc['_search_options']
                result['from_url'] = 'http://qiye.zhaopingou.com/resume/detail?resumeId={}'.format(
                    external_id.replace('zpg_', ''))
                try:
                    result['_updated'] = doc['_updated']
                except:

                    result['_updated'] = None

                collection.replace_one({'external_id': external_id}, result, True)
                logger.info('parse_success:{}'.format(external_id))
                db.update_one({'external_id': external_id}, {'$set': {'_options.update_count': 1}})

    def run(self):
        while True:
            try:
                external_id = redis_conn.lpop('zhaopingou_parse')
            except:
                logger.warning('redis is empty')
                break
            else:
                if external_id is None:
                    return
                self.to_parse(external_id)


if __name__ == "__main__":


    parse = Parser()
    #parse.to_parse('zpg_33148903')

    gevent.monkey.patch_all()
    #parse.get_id()
    p = Pool(40)
    for i in range(40):
        p.apply_async(parse.run)
    p.join()





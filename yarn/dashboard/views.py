from django.core.urlresolvers import reverse
from django.shortcuts import render, redirect
from django.conf import settings
from django.http import HttpResponse
import requests
import collections
import datetime
from collections import defaultdict

headers = {'Accept': 'application/json'}
# Create your views here.
def cluster(request, template_name='dashboard/cluster.html'):
    current_app = 'cluster'
    result = requests.get(settings.API_URL+"cluster/metrics", headers=headers).json()['clusterMetrics']
    info = requests.get(settings.API_URL+"cluster/", headers=headers).json()['clusterInfo']
        
    return render(request, template_name, locals())


def nodes(request, template_name="dashboard/nodes.html"):
    current_app = 'nodes'
    payload = request.GET.dict()
    result = requests.get(settings.API_URL+"cluster/nodes", params=payload, headers=headers).json()['nodes']
    return render(request, template_name, locals())

def process_children(element):
    # take a nested queues element and return the list of child level queues
    result = []
    for el in element['queue']:
        if el.get('queues'):
            result += process_children(el['queues'])
        else:
            result.append(el)
    return result

def queues(request, template_name="dashboard/queues.html"):
    current_app = 'queues'
    res = requests.get(settings.API_URL+"cluster/scheduler", headers=headers).json()['scheduler']['schedulerInfo']
    result = process_children(res['queues'])
    for el in result:
        el['absoluteCapacity'] = round(el['absoluteCapacity'], 2)
        el['absoluteUsedCapacity'] = round(el['absoluteUsedCapacity'], 2)
        el['load'] = round(el['absoluteUsedCapacity'] / el['absoluteCapacity'] * 100 , 2)  
        el['absoluteMaxCapacity'] = round(el['absoluteMaxCapacity'], 2)
    return render(request, template_name, locals())

def jobs(request, template_name="dashboard/jobs.html"):
    current_app = 'jobs'
    payload = request.GET.dict()
    states = ["RUNNING", "FINISHED", "FAILED", "KILLED", "NEW", "NEW_SAVING", "SUBMITTED", "ACCEPTED"]
    
    result = requests.get(settings.API_URL+"cluster/apps", params=payload, headers=headers).json()['apps']
    if result:
        result = result['app']
    else:
        result = []

    for el in result:
        el['job_id'] = el['id'].replace('application', 'job')
    return render(request, template_name, locals())


def application_master_details(request, application_id, template_name="dashboard/application_master_details.html"):
    
    url = settings.APPLICATION_API_URL.format(application_id=application_id) + "info/"
    try:
        am_details = requests.get(url, headers=headers).json()['info']
        am_details = collections.OrderedDict(sorted(am_details.items()))
        am_details['Application Jobs'] = reverse('dashboard.views.application_jobs', args=[application_id])
    except Exception as e:
        job_id = application_id.replace("application_", "job_")
        return redirect('dashboard.views.job_details', application_id=application_id, job_id=job_id)
        
    return render(request, template_name, locals())


def application_jobs(request, application_id, template_name="dashboard/application_jobs.html"):
    current_app = 'jobs'
    url = settings.APPLICATION_API_URL.format(application_id=application_id) + "jobs/"
    try:
        result = requests.get(url, headers=headers).json()['jobs']['job']
    except Exception as e:
        url = settings.HISTORY_API_URL.format(application_id=application_id) + "jobs/"
        result = requests.get(url, headers=headers).json()['jobs']['job']
    return render(request, template_name, locals())


# What is this used for???
def application_details(request, application_id, template_name="dashboard/application_details.html"):
    # show all jobs in an application
    current_app = 'jobs'
    payload = request.GET.dict()
    result = requests.get(settings.APPLICATION_API_URL.format(application_id=application_id), params=payload, headers=headers)
    result = result.json()['info']
    return render(request, template_name, locals())

def job_details(request, application_id, job_id):
    current_app = 'jobs'
    current_nav = 'overview'
    payload = request.GET.dict()
    url = settings.APPLICATION_API_URL.format(application_id=application_id)+"jobs/"+job_id
    running = True
    try:
        result = requests.get(url, params=payload, headers=headers)
        result = result.json()['job']
        result['startTime'] = datetime.datetime.fromtimestamp(result['startTime']/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        return render(request, "dashboard/active_job_details.html", locals())
    except Exception as e:
        running = False
        url = settings.HISTORY_API_URL + "jobs/" + job_id
        result = requests.get(url, params=payload, headers=headers)
        result = result.json()['job']
        result['elapsedTime'] = result['finishTime'] - result['startTime'] 
        result['startTime'] = datetime.datetime.fromtimestamp(result['startTime']/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        result['submitTime'] = datetime.datetime.fromtimestamp(result['submitTime']/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        result['finishTime'] = datetime.datetime.fromtimestamp(result['finishTime']/1000.0).strftime('%Y-%m-%d %H:%M:%S')


    return render(request, "dashboard/completed_job_details.html", locals())


def job_configuration(request, application_id, job_id, template_name="dashboard/job_configuration.html"):
    current_app = 'jobs'
    current_nav = 'conf'
    payload  = request.GET.dict()
    url = settings.APPLICATION_API_URL.format(application_id=application_id) + "jobs/{}/conf".format(job_id)
    try:
        result = requests.get(url, params=payload, headers=headers).json()['conf']['property']
    except Exception as e:
        url = settings.HISTORY_API_URL.format(application_id=application_id) +"jobs/{}/conf".format(job_id)
        result = requests.get(url, params=payload, headers=headers).json()['conf']['property']
    for el in result:
        el['source']  = ' &#8592; '.join(el['source'])
    return render(request, template_name, locals())


def job_counters(request, application_id, job_id, template_name="dashboard/job_counters.html"):
    current_app = 'jobs'
    current_nav = 'counters'
    payload  = request.GET.dict()
    url = settings.APPLICATION_API_URL.format(application_id=application_id) + "jobs/{}/counters".format(job_id)
    try:
        result = requests.get(url, params=payload, headers=headers).json()['jobCounters']
    except Exception as e:
        url = settings.HISTORY_API_URL.format(application_id=application_id) +"jobs/{}/counters".format(job_id)
        result = requests.get(url, params=payload, headers=headers).json()['jobCounters']
        
    # result = collections.OrderedDict(sorted(result.items))
    return render(request, template_name, locals())


def job_tasks(request, application_id, job_id, status=None, template_name="dashboard/job_tasks.html"):
    current_app = 'jobs'
    current_nav = 'tasks'
    payload  = request.GET.dict()
    if payload.get('type') == 'm':
        current_nav = 'map-tasks'
    elif payload.get('type') == 'r':
        current_nav = 'reduce-tasks'

    url = settings.APPLICATION_API_URL.format(application_id=application_id) + "jobs/{}/tasks".format(job_id)
    try:
        response = requests.get(url, params=payload, headers=headers).json()
        result = response['tasks']
    except Exception as e:
        url = settings.HISTORY_API_URL.format(application_id=application_id) +"jobs/{}/tasks".format(job_id)
        result = requests.get(url, params=payload, headers=headers).json()
        result = result['tasks']

    if result:
        result = result['task']
        if status:
            result = [el for el in result if el['state'] == status ]
    return render(request, template_name, locals())


# task_details is just showing all attempts in the task(task_attempts)
# this is analogous to task_attempts
def task_details(request, application_id, job_id, task_id, template_name="dashboard/task_details.html"):
    current_app = 'jobs'
    current_nav = 'overview'
    payload  = request.GET.dict()

    url_suffix = "jobs/{}/tasks/{}/attempts".format(job_id, task_id)
    try:
        url = settings.HISTORY_API_URL.format(application_id=application_id) + url_suffix
        response = requests.get(url, params=payload, headers=headers).json()
        result = response['taskAttempts']['taskAttempt']
    except Exception as e:
        #go to history_server
        url = settings.APPLICATION_API_URL.format(application_id=application_id) + url_suffix
        response = requests.get(url, params=payload, headers=headers)
        try:
            response = response.json()
            result = response['taskAttempts']['taskAttempt']
        except Exception as e:
            result = response.text
            return HttpResponse(result, content_type='text/html')
        
    return render(request, template_name, locals())


def task_counters(request, application_id, job_id, task_id, template_name="dashboard/task_counters.html"):
    current_app = 'jobs'
    current_nav = 'counters'
    payload  = request.GET.dict()
    url_suffix = "jobs/{}/tasks/{}/counters".format(job_id, task_id)
    try:
        url = settings.APPLICATION_API_URL.format(application_id=application_id) + url_suffix
        response = requests.get(url, params=payload, headers=headers).json()
        result = response['jobTaskCounters']['taskCounterGroup']
    except Exception as e:
        url = settings.HISTORY_API_URL.format(application_id=application_id) + url_suffix
        response = requests.get(url, params=payload, headers=headers).json()
        result = response['jobTaskCounters']['taskCounterGroup']
    return render(request, template_name, locals())


# There is no attempt details, all attempt details are shown in the task_details/ task_attempts page in a table
def attempt_counters(request, application_id, job_id, task_id, attempt_id, template_name="dashboard/attempt_counters.html"):
    url_suffix = "jobs/{}/tasks/{}/attempts/{}/counters".format(job_id, task_id, attempt_id)
    payload  = request.GET.dict()
    current_app = 'jobs'
    try:
        url = settings.APPLICATION_API_URL.format(application_id=application_id) + url_suffix
        response = requests.get(url, params=payload, headers=headers).json()
        result = response['jobTaskAttemptCounters']['taskAttemptCounterGroup']
    except Exception as e:
        url = settings.HISTORY_API_URL.format(application_id=application_id) + url_suffix
        response = requests.get(url, params=payload, headers=headers).json()
        result = response['jobTaskAttemptCounters']['taskAttemptCounterGroup']
    return render(request, template_name, locals())

cache = None
def hack(request):
    # global cache
    # if cache:
    #     return  HttpResponse(cache, content_type='text/html')

    from django.db import connection
    cursor = connection.cursor()
    cursor.execute("SELECT t, tgt_rack, sum(xfer) AS incoming_total, sum(if(src_rack = tgt_rack, xfer, 0)) AS incoming_intra_rack, sum(if(src_rack = tgt_rack, 0, xfer)) AS incoming_inter_rack FROM (SELECT FROM_UNIXTIME(floor(time_bucket/600)*600) AS t, IFNULL(src.rack, 'EXTERNAL') AS src_rack, IFNULL(tgt.rack, 'EXTERNAL') AS tgt_rack, sum(bytes_sent) AS xfer FROM cluster_usage_history u LEFT JOIN hostdata src ON u.source_ip = src.ip LEFT JOIN hostdata tgt ON u.target_ip = tgt.ip GROUP BY t, src_rack, tgt_rack) master GROUP BY t, tgt_rack")
    rows = cursor.fetchall()
    racks = {}
    racks_intra = {} # { "rack1": {"10:00" : 200, "10:10" : 100}}
    racks_inter = {}
    print len(rows)
    for row in rows:
        t_bucket = row[0] 
        rack_name = row[1]
        incoming_total= row[2]
        incoming_intra_rack = row[3]
        incoming_inter_rack = row[4]
        if racks_intra.get(rack_name):
            racks_intra[rack_name][t_bucket] = incoming_intra_rack
        else:
            racks_intra[rack_name] = {t_bucket:incoming_intra_rack}
        
        if racks_inter.get(rack_name):
            racks_inter[rack_name][t_bucket] = incoming_inter_rack
        else:
            racks_inter[rack_name] = {t_bucket:incoming_inter_rack}

    cursor.execute("""SELECT t, src_rack, sum(xfer) as outgoing_total, sum(IF(src_rack = tgt_rack, xfer, 0)) AS outgoing_intra_rack,sum(IF(src_rack = tgt_rack, 0, xfer)) AS outgoing_inter_rack
         FROM
           (SELECT FROM_UNIXTIME(floor(time_bucket/600)*600) AS t, IFNULL(src.rack, 'EXTERNAL') AS src_rack, IFNULL(tgt.rack, 'EXTERNAL') AS tgt_rack, sum(bytes_sent) AS xfer
            FROM cluster_usage_history u
            LEFT JOIN hostdata src ON u.source_ip = src.ip
            LEFT JOIN hostdata tgt ON u.target_ip = tgt.ip
            GROUP BY t, src_rack, tgt_rack) master
         GROUP BY t, src_rack""")
    rows = cursor.fetchall()
    print len(rows)
    for row in rows:
        t_bucket = row[0] 
        rack_name = row[1]
        outgoing_total= row[2]
        outgoing_intra_rack = row[3]
        outgoing_inter_rack = row[4]
        
        if racks_intra.get(rack_name):
            if racks_intra[rack_name].get(t_bucket):
                racks_intra[rack_name][t_bucket] = racks_intra[rack_name][t_bucket] + outgoing_intra_rack
            else:
                racks_intra[rack_name][t_bucket] = outgoing_intra_rack
        else:
            racks_intra[rack_name] = {t_bucket:outgoing_intra_rack}
        
        if racks_inter.get(rack_name):
            if racks_inter[rack_name].get(t_bucket):
                racks_inter[rack_name][t_bucket] = racks_inter[rack_name][t_bucket] + outgoing_inter_rack
            else:
                racks_inter[rack_name][t_bucket] = outgoing_inter_rack
        else:
            racks_inter[rack_name] = {t_bucket:outgoing_inter_rack}
        
        
    
    coords = {"/lhr1/206": ",100,100", "/lhr1/207" : ",200,100", "/lhr1/301": ",300,100", "/lhr1/302": ",400,100", "/lhr1/101":",500,100", "/lhr1/103":",200,300", "/lhr1/104":",300,300", "/lhr1/105":",400,300", "/lhr1/106":",500,300", "/lhr1/107":",100,300" }
    connection.close()
    response = """hostId,cx,cy,00:00,00:10,00:20,00:30,00:40,00:50,01:00,01:10,01:20,01:30,01:40,01:50,02:00,02:10,02:20,02:30,02:40,02:50,03:00,03:10,03:20,03:30,03:40,03:50,04:00,04:10,04:20,04:30,04:40,04:50,05:00,05:10,05:20,05:30,05:40,05:50,06:00,06:10,06:20,06:30,06:40,06:50,07:00,07:10,07:20,07:30,07:40,07:50,08:00,08:10,08:20,08:30,08:40,08:50,09:00,09:10,09:20,09:30,09:40,09:50,10:00,10:10,10:20,10:30,10:40,10:50,11:00,11:10,11:20,11:30,11:40,11:50,12:00,12:10,12:20,12:30,12:40,12:50,13:00,13:10,13:20,13:30,13:40,13:50,14:00,14:10,14:20,14:30,14:40,14:50,15:00,15:10,15:20,15:30,15:40,15:50,16:00,16:10,16:20,16:30,16:40,16:50,17:00,17:10,17:20,17:30,17:40,17:50,18:00,18:10,18:20,18:30,18:40,18:50,19:00,19:10,19:20,19:30,19:40,19:50,20:00,20:10,20:20,20:30,20:40,20:50,21:00,21:10,21:20,21:30,21:40,21:50,22:00,22:10,22:20,22:30,22:40,22:50,23:00,23:10,23:20,23:30,23:40,23:50\n"""
    for rack_name in racks_inter:
        if rack_name == "EXTERNAL":
            continue
        res = rack_name + coords[rack_name]
        data = racks_inter[rack_name]
        for t_bucket in sorted(data.keys()):
            res = res + ",-" + str(data[t_bucket])
        response = response + res +"\n"

    for rack_name in racks_intra:
        if rack_name == "EXTERNAL":
            continue
        res = rack_name + coords[rack_name]
        data = racks_intra[rack_name]
        for t_bucket in sorted(data.keys()):
            res = res + "," + str(data[t_bucket])
        response = response + res + "\n"
    cache = response
    return  HttpResponse(response, content_type='text/html')
        
cache2 = None
def hack2(request):
    global cache2
    if cache2:
        return HttpResponse(cache2, content_type="text/html")
    from django.db import connection
    cursor = connection.cursor()
    cursor.execute("select t, src_rack, tgt_rack, sum(xfer) as inter_rack_xfer from (SELECT FROM_UNIXTIME(floor(time_bucket/600)*600) AS t, IFNULL(src.rack, 'EXTERNAL') AS src_rack, IFNULL(tgt.rack, 'EXTERNAL') AS tgt_rack, sum(bytes_sent) AS xfer FROM cluster_usage_history u LEFT JOIN hostdata src ON u.source_ip = src.ip LEFT JOIN hostdata tgt ON u.target_ip = tgt.ip  GROUP BY t, src_rack, tgt_rack) master GROUP BY t, src_rack, tgt_rack ORDER BY 1,2,3")
    rows = cursor.fetchall()
    connection.close()
    coords = {"/lhr1/101":"100", "/lhr1/103":"200", "/lhr1/104":"300", "/lhr1/105":"400", "/lhr1/106":"500", "/lhr1/107":"600", "/lhr1/206": "700", "/lhr1/207" : "800", "/lhr1/301": "900", "/lhr1/302": "1000"}
    response = """hostId,cx,cy,00:00,00:10,00:20,00:30,00:40,00:50,01:00,01:10,01:20,01:30,01:40,01:50,02:00,02:10,02:20,02:30,02:40,02:50,03:00,03:10,03:20,03:30,03:40,03:50,04:00,04:10,04:20,04:30,04:40,04:50,05:00,05:10,05:20,05:30,05:40,05:50,06:00,06:10,06:20,06:30,06:40,06:50,07:00,07:10,07:20,07:30,07:40,07:50,08:00,08:10,08:20,08:30,08:40,08:50,09:00,09:10,09:20,09:30,09:40,09:50,10:00,10:10,10:20,10:30,10:40,10:50,11:00,11:10,11:20,11:30,11:40,11:50,12:00,12:10,12:20,12:30,12:40,12:50,13:00,13:10,13:20,13:30,13:40,13:50,14:00,14:10,14:20,14:30,14:40,14:50,15:00,15:10,15:20,15:30,15:40,15:50,16:00,16:10,16:20,16:30,16:40,16:50,17:00,17:10,17:20,17:30,17:40,17:50,18:00,18:10,18:20,18:30,18:40,18:50,19:00,19:10,19:20,19:30,19:40,19:50,20:00,20:10,20:20,20:30,20:40,20:50,21:00,21:10,21:20,21:30,21:40,21:50,22:00,22:10,22:20,22:30,22:40,22:50,23:00,23:10,23:20,23:30,23:40,23:50\n"""
    result = defaultdict(list)
    import pdb; pdb.set_trace();
    for row in rows:
        time_bucket = row[0]
        src = row[1]
        tgt = row[2]
        if src == 'EXTERNAL' or tgt == 'EXTERNAL':
            continue

        data = row[3] if src != tgt else -row[3]

        result[src + "->" + tgt].append((time_bucket, data))

    for key in result:
        response = response + key + "," + coords[key.split("->")[0]] + "," + coords[key.split("->")[1]] 
        for t, d in result[key]:
            response = response + ","  + str(d)
        response = response + "\n"
    cache2 = response
    return HttpResponse(response, content_type="text/html")
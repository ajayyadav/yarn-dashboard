from django.core.urlresolvers import reverse
from django.shortcuts import render
from django.conf import settings
import requests
import collections
import datetime

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
        #for what all status should we send the request here??
        result = []

    for el in result:
        el['job_id'] = el['id'].replace('application', 'job')
    return render(request, template_name, locals())


def application_master_details(request, application_id, template_name="dashboard/application_master_details.html"):
    
    url = settings.APPLICATION_API_URL.format(application_id=application_id) + "info/"
    print url 
    am_details = requests.get(url, headers=headers).json()['info']
    am_details = collections.OrderedDict(sorted(am_details.items()))
    am_details['Application Jobs'] = reverse('dashboard.views.application_jobs', args=[application_id])
    return render(request, template_name, locals())


def application_jobs(request, application_id, template_name="dashboard/application_jobs.html"):
    current_app = 'jobs'
    url = settings.APPLICATION_API_URL.format(application_id=application_id) + "jobs/"
    try:
        result = requests.get(url, headers=headers).json()['jobs']['job']
    except (TypeError, KeyError) as e:
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
        result = requests.get(url, params=payload, headers=headers).json()['job']
        result['startTime'] = datetime.datetime.fromtimestamp(result['startTime']/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        return render(request, "dashboard/active_job_details.html", locals())
    except ValueError:
        running = False
        result = requests.get(settings.HISTORY_API_URL+"jobs/"+job_id, params=payload, headers=headers).json()['job']
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
    except (ValueError, TypeError, KeyError) as e:
        url = settings.HISTORY_API_URL.format(application_id=application_id) +"jobs/{}/conf".format(job_id)
        result = requests.get(url, params=payload, headers=headers).json()['conf']['property']
        
    return render(request, template_name, locals())


def job_counters(request, application_id, job_id, template_name="dashboard/job_counters.html"):
    current_app = 'jobs'
    current_nav = 'counters'
    payload  = request.GET.dict()
    url = settings.APPLICATION_API_URL.format(application_id=application_id) + "jobs/{}/counters".format(job_id)
    try:
        result = requests.get(url, params=payload, headers=headers).json()['jobCounters']
    except (ValueError, TypeError, KeyError) as e:
        url = settings.HISTORY_API_URL.format(application_id=application_id) +"jobs/{}/counters".format(job_id)
        result = requests.get(url, params=payload, headers=headers).json()['jobCounters']
        
    # result = collections.OrderedDict(sorted(result.items))
    return render(request, template_name, locals())


def job_tasks(request, application_id, job_id, template_name="dashboard/job_tasks.html"):
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
    except (ValueError, TypeError, KeyError) as e:
        url = settings.HISTORY_API_URL.format(application_id=application_id) +"jobs/{}/tasks".format(job_id)
        result = requests.get(url, params=payload, headers=headers).json()
        result = result['tasks']

    if result:
        result = result['task']
            
    return render(request, template_name, locals())


# task_details is just showing all attempts in the task(task_attempts)
def task_details(request, application_id, job_id, task_id, template_name="dashboard/task_details.html"):
    current_app = 'jobs'
    current_nav = 'overview'
    payload  = request.GET.dict()
    

    url_suffix = "jobs/{}/tasks/{}/attempts".format(job_id, task_id)
    try:
        url = settings.HISTORY_API_URL.format(application_id=application_id) + url_suffix
        response = requests.get(url, params=payload, headers=headers).json()
        result = response['taskAttempts']['taskAttempt']
    except (TypeError, KeyError) as e:
        #go to history_server
        url = settings.APPLICATION_API_URL.format(application_id=application_id) + url_suffix
        response = requests.get(url, params=payload, headers=headers).json()
        result = response['taskAttempts']['taskAttempt']
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
    except Exception, e:
        url = settings.HISTORY_API_URL.format(application_id=application_id) + url_suffix
        response = requests.get(url, params=payload, headers=headers).json()
        result = response['jobTaskCounters']['taskCounterGroup']
    return render(request, template_name, locals())


# There is no attempt details, all attempt details are shown in the task_details/ task_attempts page in a table
def attempt_logs(request):
    pass
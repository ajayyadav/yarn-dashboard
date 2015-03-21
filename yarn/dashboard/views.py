from django.shortcuts import render
from django.conf import settings
import requests

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
    return render(request, template_name, locals())

def jobs(request, template_name="dashboard/jobs.html"):
    current_app = 'jobs'
    payload = request.GET.dict()
    result = requests.get(settings.API_URL+"cluster/apps", params=payload, headers=headers).json()['apps']
    if result:
        result = result['app']
    else:
        result = []

    for el in result:
        el['job_id'] = el['id'].replace('application', 'job')
    return render(request, template_name, locals())


def application_master_details(request, application_id, template_name="dashboard/application_master_details.html"):
    current_app = 'jobs'
    result = requests.get(settings.APPLICATION_API_URL.format(application_id), headers=headers).json()
    return render(request, template_name, locals())

def application_details(request, application_id, template_name="dashboard/application_details.html"):
    # show all jobs in an application
    current_app = 'jobs'
    payload = request.GET.dict()
    result = requests.get(settings.APPLICATION_API_URL.format(application_id=application_id), params=payload, headers=headers).json()
    return render(request, template_name, locals())

def job_details(request, application_id, job_id):
    current_app = 'jobs'
    payload = request.GET.dict()
    url = settings.APPLICATION_API_URL.format(application_id=application_id)+"jobs/"+job_id
    running = True
    try:
        result = requests.get(url, params=payload, headers=headers).json()['job']
        return render(request, "dashboard/active_job_details.html", locals())
    except ValueError:
        running = False
        result = requests.get(settings.HISTORY_API_URL+"jobs/"+job_id, params=payload, headers=headers).json()['job']
    return render(request, "dashboard/completed_job_details.html", locals())




import requests
from django.shortcuts import render
from django.conf import settings
from django.http import HttpResponse

# Create your views here.
def configuration(request):
	response =  requests.get(settings.SERVER_CONF_URL).text
	return HttpResponse(response, content_type="application/xml")


def stacks(request):
	response =  requests.get(settings.SERVER_STACK_URL).text
	return HttpResponse(response, content_type="text/plain")



def logs(request):
	response =  requests.get(settings.SERVER_LOG_URL).text
	return HttpResponse(response, content_type="text/html")
	

def metrics(request):
	pass

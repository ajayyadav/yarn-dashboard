from django.conf import settings

def global_settings(request):
	return {'API_SERVER_URL': settings.API_SERVER_URL, 'API_URL':settings.API_URL}
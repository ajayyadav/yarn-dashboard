from django.conf.urls import patterns, include, url


urlpatterns = patterns('server.views',
    # 
    url(r'^conf$', 'configuration', name="configuration"),
    url(r'^stacks$', 'stacks', name="stacks"),
    url(r'^logs$', 'logs', name="logs"),

)
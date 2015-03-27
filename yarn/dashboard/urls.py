from django.conf.urls import patterns, include, url


urlpatterns = patterns('dashboard.views',
    # url(r'^(?P<type>[a-z]+)/(?P<name>[0-9a-zA-Z@\-]+)/$', 'entity_details', name="entity_details"),
    url(r'^$', 'cluster', name="cluster"),
    url(r'^nodes$', 'nodes', name="nodes"),
    url(r'^queues$', 'queues', name="queues"),
    url(r'^jobs', 'jobs', name="jobs"),
    url(r'^apps/(?P<application_id>)/', 'application_master_details', name="am_details"),
    url(r'^app/(?P<application_id>)/', 'application_details', name="application_details"),
    url(r'^app/(?P<application_id>\w+)/job/(?P<job_id>\w+)/', 'job_details', name="job_details"),
    url(r'^apps/(?P<application_id>\w+)/(?P<job_id>\w+)/tasks', 'tasks_list', name="tasks_list"),
)
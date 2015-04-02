from django.conf.urls import patterns, include, url


urlpatterns = patterns('dashboard.views',
    # url(r'^(?P<type>[a-z]+)/(?P<name>[0-9a-zA-Z@\-]+)/$', 'entity_details', name="entity_details"),
    url(r'^$', 'cluster', name="cluster"),
    url(r'^nodes$', 'nodes', name="nodes"),
    url(r'^queues$', 'queues', name="queues"),
    url(r'^jobs', 'jobs', name="jobs"),
    url(r'^apps/(?P<application_id>\w+)/$', 'application_master_details', name="application_master_details"),
    
    # url(r'^app/(?P<application_id>\w+)/$', 'application_details', name="application_details"),
    url(r'^app/(?P<application_id>\w+)/jobs/$', 'application_jobs', name="application_jobs"),

    #Job related URLs
    url(r'^app/(?P<application_id>\w+)/job/(?P<job_id>\w+)$', 'job_details', name="job_details"),
    url(r'^app/(?P<application_id>\w+)/job/(?P<job_id>\w+)/conf/$', 'job_configuration', name="job_configuration"),
    url(r'^app/(?P<application_id>\w+)/job/(?P<job_id>\w+)/counters/$', 'job_counters', name="job_counters"),
    url(r'^app/(?P<application_id>\w+)/job/(?P<job_id>\w+)/tasks$', 'job_tasks', name="job_tasks"),

    #Task related URLs
    url(r'^app/(?P<application_id>\w+)/job/(?P<job_id>\w+)/tasks/(?P<task_id>\w+)/$', 'task_details', name="task_details"),
    url(r'^app/(?P<application_id>\w+)/job/(?P<job_id>\w+)/tasks/(?P<task_id>\w+)/counters/$', 'task_counters', name="task_counters"),

)
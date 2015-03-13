from django.conf.urls import patterns, include, url

# urlpatterns = patterns('',
#     # url(r'^process/create/$', CreateProcess.as_view(), name="create_process"),
#     # url(r'^cluster/(?P<name>[0-9a-zA-Z@\-]+)/edit', UpdateCluster.as_view(), name="edit_cluster"),
# )

urlpatterns = patterns('dashboard.views',
    # url(r'^(?P<type>[a-z]+)/(?P<name>[0-9a-zA-Z@\-]+)/$', 'entity_details', name="entity_details"),
    url(r'^$', 'cluster', name="cluster"),
    url(r'^nodes$', 'nodes', name="nodes"),
    url(r'^queues$', 'queues', name="queues"),
    url(r'^jobs', 'jobs', name="jobs"),
)
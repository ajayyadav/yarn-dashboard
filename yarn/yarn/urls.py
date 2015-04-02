from django.conf.urls import patterns, include, url
from django.contrib import admin

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'yarn.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^dashboard/', include('dashboard.urls')),
)

from django.conf.urls import patterns, include, url

from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',

    url(r'^dashboard/', include('dashboard.urls')),
    url(r'^server/', include('server.urls')),
        
)

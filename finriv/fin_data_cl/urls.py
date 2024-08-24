from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('complex_analysis/', views.complex_analysis, name='complex_analysis'),
    path('about/', views.about, name='about'),
    path('contact/', views.contact, name='contact'),
    path('get_data/', views.get_data, name='get_data'),
    path('ticker_suggestions/', views.ticker_suggestions, name='ticker_suggestions'),
    path('filter_ratios/', views.filter_ratios, name='filter_ratios'),  # New URL for filtering ratios
]


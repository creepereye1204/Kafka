# app/urls.py
from django.urls import path
from .views import ProduceUserProfile

urlpatterns = [
    path("produce/", ProduceUserProfile.as_view(), name="produce-user-profile"),
]

# app/views.py
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings

from .serializers import UserProfileSerializer
from .kafka_client import send


class ProduceUserProfile(APIView):
    """
    POST /api/produce/
    Body(JSON): UserProfileSerializer 스키마
    202 Accepted 즉시 반환 후 비동기 전송
    """

    def post(self, request):
        serializer = UserProfileSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.data

        topic = request.query_params.get("topic", getattr(
            settings, "KAFKA_DEFAULT_TOPIC", "user-profile"))
        key = data.get("user_id")
        headers = {
            "content-type": "application/json",
            "schema": "user-profile-v1",
        }

        send(topic=topic, key=key, value=data, headers=headers)
        return Response({"status": "accepted", "topic": topic, "key": key}, status=status.HTTP_202_ACCEPTED)

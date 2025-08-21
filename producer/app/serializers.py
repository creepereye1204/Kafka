# app/serializers.py
from rest_framework import serializers
from uuid import uuid4
from datetime import datetime, timezone


class UserProfileSerializer(serializers.Serializer):
    user_id = serializers.CharField(
        required=False, default=lambda: str(uuid4()))
    username = serializers.CharField(min_length=2, max_length=50)
    email = serializers.EmailField()
    age = serializers.IntegerField(
        required=False, min_value=0, max_value=150, allow_null=True)
    interests = serializers.ListField(
        child=serializers.CharField(), required=False, default=list)
    is_active = serializers.BooleanField(required=False, default=True)
    created_at = serializers.DateTimeField(
        required=False, default=lambda: datetime.now(timezone.utc))
    updated_at = serializers.DateTimeField(required=False, allow_null=True)

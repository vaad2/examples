import datetime
import logging
import os

from constance import config as cfg
from django.db import transaction
from django.db.models import Count, Sum
from django.utils.translation import gettext_lazy as _
from drf_spectacular.utils import extend_schema
from rest_framework import decorators
from rest_framework import exceptions as drf_exceptions
from rest_framework import response, status
from rest_framework import viewsets as drf_viewsets

from server.apps.acl import models as acl_models
from server.apps.acl import permissions as acl_permissions
from server.apps.common import drf as common_drf
from server.apps.common import renderers as common_renderers
from server.apps.common.services import notification_manager
from server.apps.market import choices as market_choices
from server.apps.market import filters as market_filters
from server.apps.market import models as market_models
from server.apps.market import services as market_services
from server.apps.market import utils as market_utils
from server.apps.market.serializers import v1 as market_serializers

logger = logging.getLogger(__name__)

notification_manager = notification_manager.NotificationManager()


class CardViewSet(
    common_drf.dynamic_viewset(
        market_models.Card,
    )
):
    _permissions_map = {
        "default": [
            acl_permissions.RolePermissionChecker(
                roles=["self"],
            )
        ],
        "delete": [
            acl_permissions.RolePermissionChecker(
                roles=["admin", "staff", "self"],
            ),
        ],
        "stat": [acl_permissions.RolePermissionChecker(roles=["self"])],
    }

    _serializers_map = {
        "stat": market_serializers.ViewCardStatSerializer,
        "default": market_serializers.CardCreateSerializer,
    }

    _filterset_classes = {
        "stat": market_filters.CardStatFilter,
    }

    search_fields = [
        "id",
        "user__username",
        "number",
        "currency__code",
        "fio",
        "phone_number",
    ]

    def get_queryset(self):
        if self.action == "stat":
            return (
                market_models.ViewCardStat.objects.filter(
                    user=self.request.user,
                )
                .values(
                    "id",
                    "number",
                    "currency_id",
                    "user_id",
                    "fio",
                    "phone_number",
                    "current_amount",
                    "limit_amount",
                    "limit_amount_contract",
                    "limit_amount_24h",
                    "limit_qty_24h",
                    "current_qty_24h",
                    "is_limit_amount_24h",
                    "frozen_sum",
                    "bank",
                    "bank_id",
                    "description",
                    "is_published",
                )
                .annotate(
                    order_count=Count("id"),
                    day_sum=Sum("dst_amount"),
                )
            )

        return market_models.Card.objects.filter(
            user=self.request.user
        ).order_by("-created_at")

    @extend_schema(
        responses=market_serializers.ViewCardStatSerializer(many=True),
    )
    @decorators.action(
        methods=["get"],
        detail=False,
    )
    def stat(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    @decorators.action(
        methods=["post"],
        detail=True,
    )
    def limits_reset(self, request, *args, **kwargs):
        card = self.get_object()

        market_models.CardCurrentLimit.objects.filter(
            card=card, date=datetime.date.today()
        ).delete()

        card.current_amount = 0

        card.save(update_fields=["current_amount"])

        return response.Response(status=200)

    def destroy(self, request, *args, **kwargs):
        card = self.get_object()
        card.is_active = False
        safe_delete_prefix = f"deleted_{card.id}_"
        card.number = safe_delete_prefix + card.number
        card.save(update_fields=["is_active", "number"])
        return response.Response(status=204)


class BankViewSet(
    common_drf.dynamic_viewset(
        market_models.Bank,
        base_model_viewset=drf_viewsets.ReadOnlyModelViewSet,
    )
):
    _permissions_map = {
        "default": [
            acl_permissions.RolePermissionChecker(
                roles=["self"],
            )
        ]
    }


class CurrencyViewSet(
    common_drf.dynamic_viewset(
        market_models.Currency,
        base_model_viewset=drf_viewsets.ReadOnlyModelViewSet,
    )
):
    _permissions_map = {
        "default": [
            acl_permissions.RolePermissionChecker(
                roles=["self"],
            )
        ]
    }


class CurrencyRateViewSet(
    common_drf.dynamic_viewset(
        market_models.CurrencyRate,
        base_model_viewset=drf_viewsets.ReadOnlyModelViewSet,
    )
):
    serializer_class = market_serializers.CurrencyRateSerializer
    _permissions_map = {
        "default": [
            acl_permissions.RolePermissionChecker(
                roles=["self"],
            )
        ]
    }

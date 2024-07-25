from operator import itemgetter

from django.urls import resolve
from drf_spectacular.generators import SchemaGenerator
from drf_spectacular.openapi import AutoSchema as SpcAutoSchema
from drf_spectacular.plumbing import get_doc
from rest_framework import serializers, status, viewsets
from rest_framework.schemas.openapi import AutoSchema

from server.apps.common import mixins as common_mixins


def dynamic_serializer(
    class_name, model, base_model=serializers.ModelSerializer, fields="__all__"
):
    cls_attrs = type("Meta", (object,), {"model": model, "fields": fields})
    return type(class_name, (base_model,), {"Meta": cls_attrs})


def dynamic_viewset(
    model,
    base_class_name=None,
    base_model_serializer=serializers.ModelSerializer,
    base_model_viewset=viewsets.ModelViewSet,
    serializer_class_name=None,
    viewset_class_name=None,
):
    base_class_name = base_class_name or "Base" + model.__name__.capitalize()

    viewset_class_name = viewset_class_name or (base_class_name + "ViewSet")
    serializer_class_name = serializer_class_name or (
        base_class_name + "Serializer"
    )

    cls_serializer = dynamic_serializer(
        serializer_class_name, model, base_model_serializer
    )

    return type(
        viewset_class_name,
        (
            common_mixins.ViewSetMixin,
            base_model_viewset,
        ),
        {"queryset": model.objects.all(), "serializer_class": cls_serializer},
    )

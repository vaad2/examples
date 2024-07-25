# pylint: disable=too-many-statements # TODO: GCLOUD2-4774
import inspect
import json
import sys
from datetime import date, datetime
from typing import List
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import factory as factory_boy
import pytest
from dateutil.relativedelta import relativedelta
from mock_alchemy.mocking import UnifiedAlchemyMagicMock
from pytest_mock import MockerFixture
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from api.serializers.enums import (
    AIClusterStatusEnum,
    LifecyclePolicyActionEnum,
    LifecyclePolicyStatusesEnum,
)
from db.entities.ai import AICluster
from db.entities.billing_reservation import (
    BillingReservation,
    BillingResource,
    BillingResourceReservation,
    BillingSubscription,
)
from db.entities.client import Client
from db.entities.keypair import Keypair
from db.entities.keystone import Keystone
from db.entities.lifecyclepolicy_and_schedule import (
    LifecyclePolicy,
    LifecyclePolicyVolumeRelationship,
    Schedule,
)
from db.entities.project import Project
from db.entities.quota import (
    DEFAULT_PERCENTAGE_OF_THE_QUOTA_FOR_NOTIFICATION,
    GlobalQuotas,
    LimitRequestAutoApproveThreshold,
    LimitRequests,
    QuotaNotificationThreshold,
    RegionalQuotas,
)
from db.entities.region import Region
from db.entities.region_access import RegionAccess
from db.entities.reseller_name_templates import (
    ResellerNameTemplates,
)
from db.entities.role_assignment import Role, RoleAssignment
from db.entities.service_endpoint import ServiceEndpoint
from db.entities.user_action import UserAction
from db.enums import (
    ProductStatus,
    RegionAccessType,
    RegionState,
)
from db.task import OpenstackTask
from settings.settings import DB_OPTIONS
from tests.common import constants
from tests.common.test_utils import get_timestamp_as_id
from worker.task_scheduler import TaskScheduler

# pylint: disable=redefined-outer-name


@pytest.fixture(scope="session")
def db_engine():
    options = DB_OPTIONS.copy()
    return create_engine(options.pop("uri"), **options)


@pytest.fixture(scope="session")
def db_connection(db_engine):
    connection = db_engine.connect()
    yield connection
    connection.close()


@pytest.fixture(scope="function")
def db_session(db_connection):
    transaction = db_connection.begin()
    session_factory = sessionmaker(bind=db_connection)
    session = session_factory()
    yield session
    session.close()
    transaction.rollback()


@pytest.fixture(scope="function")
def mock_db_session_managed(mocker: MockerFixture, db_session):
    func_name_to_patch = "get_session_managed"
    for module_name, module in list(sys.modules.items()):
        if not inspect.ismodule(module):
            continue
        for obj_name, obj in module.__dict__.items():
            # noinspection PyBroadException
            try:  # need that as inspect may raise for some modules
                if inspect.isfunction(obj) and obj_name == func_name_to_patch:
                    session_managed = mocker.patch(f"{module_name}.{func_name_to_patch}")
                    session_managed.return_value.__enter__.return_value = db_session
            except Exception:  # pylint: disable=broad-except
                continue
    yield db_session


@pytest.fixture
def mock_db_session(mocker):
    session = UnifiedAlchemyMagicMock()
    mocker.patch("db.utils.get_session", return_value=session)
    yield session
    session.close()


@pytest.fixture
def base_factory(db_session):
    class BaseFactory(factory_boy.alchemy.SQLAlchemyModelFactory):
        class Meta:
            abstract = True
            sqlalchemy_session = db_session
            sqlalchemy_session_persistence = factory_boy.alchemy.SESSION_PERSISTENCE_FLUSH

    return BaseFactory


@pytest.fixture(name="project_factory")
def _project_factory(base_factory, client_factory):
    class ProjectFactory(base_factory):
        id = factory_boy.Sequence(lambda n: get_timestamp_as_id() - n)
        client_id = factory_boy.LazyAttribute(lambda _: client_factory().id)

        class Meta:
            model = Project

    return ProjectFactory


@pytest.fixture
def keystone_factory(base_factory):
    class KeystoneFactory(base_factory):
        id = factory_boy.Sequence(lambda n: get_timestamp_as_id() - n)
        url = "foobar"
        state = "NEW"
        keystone_federated_domain_id = "foobar"

        class Meta:
            model = Keystone

    return KeystoneFactory


@pytest.fixture(name="service_endpoint_factory")
def _service_endpoint_factory(base_factory):
    class ServiceEndpointFactory(base_factory):
        url = factory_boy.Sequence(lambda n: f"https://api.preprod.world/security/iaas{get_timestamp_as_id() - n}")
        service = "ddos"
        admin_username = ""
        admin_password = ""

        class Meta:
            model = ServiceEndpoint

    return ServiceEndpointFactory


@pytest.fixture(name="region_factory")
def _region_factory(base_factory, keystone_factory, service_endpoint_factory):
    class RegionFactory(base_factory):
        id = factory_boy.Sequence(lambda n: get_timestamp_as_id() - n)
        display_name = factory_boy.Sequence(lambda n: str(get_timestamp_as_id() - n))
        keystone = keystone_factory.create()
        keystone_name = keystone.id
        state = RegionState.ACTIVE
        access_level = RegionAccessType.CORE
        ddos_endpoint = service_endpoint_factory.create()

        class Meta:
            model = Region

    return RegionFactory


@pytest.fixture(name="client_factory")
def _client_factory(base_factory):
    class ClientFactory(base_factory):
        id = factory_boy.Sequence(lambda n: get_timestamp_as_id() - n)
        reseller_id = id
        product_status = ProductStatus.ACTIVE

        class Meta:
            model = Client

    return ClientFactory


@pytest.fixture(name="aicluster_factory")
def _aicluster_factory(base_factory, client_factory, region_factory, project_factory):
    client = client_factory.create()
    region = region_factory.create()
    project = project_factory.create()

    class AIClusterFactory(base_factory):
        cluster_id = factory_boy.Sequence(lambda n: get_timestamp_as_id() - n)
        name = "test_cluster"
        status = AIClusterStatusEnum.ACTIVE
        interfaces = "[{}]"
        creator_task_id = factory_boy.Sequence(lambda n: get_timestamp_as_id() - n)
        task_id = factory_boy.Sequence(lambda n: get_timestamp_as_id() - n)
        client_id = client.id
        region_id = region.id
        project_id = project.id
        image_id = "1"
        flavor_name = "1"
        vipu_version = "1.0"
        poplar_sdk_version = "1.0"

        class Meta:
            model = AICluster

    return AIClusterFactory


@pytest.fixture
def global_quota_factory(base_factory, client_factory):
    class GlobalQuotaFactory(base_factory):
        client_id = factory_boy.LazyAttribute(lambda _: client_factory().id)
        project_count_usage = 1
        project_count_limit = 2
        keypair_count_usage = 0
        keypair_count_limit = 100

        class Meta:
            model = GlobalQuotas

    return GlobalQuotaFactory



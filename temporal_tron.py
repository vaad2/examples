from decimal import Decimal
from typing import List, Dict, Tuple

from temporalio import activity
from temporalio.exceptions import ApplicationError

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import (
    Column,
    Integer,
    String,
    Numeric,
    Boolean,
    desc,
    select,
    update,
    ForeignKey,
)

import aiohttp
from aiolimiter import AsyncLimiter
from settings import (
    DATABASE_URL,
    TRON_API_URL,
    USDT_CONTRACT_ADDRESS,
    MIN_TRX_FOR_FEES,
    GAS_RESERVE_ADDRESSES,
)

# Database setup
engine = create_async_engine(
    DATABASE_URL, echo=True, pool_size=20, max_overflow=0
)
async_session = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()


class InnerAddress(Base):
    __tablename__ = "pks_market_address"

    id = Column(Integer, primary_key=True, autoincrement=True)
    is_external = Column(Boolean, default=False, nullable=False)
    address = Column(String(255), unique=True, nullable=False)
    amount = Column(Integer, default=0, index=True, nullable=False)
    is_aml_ban = Column(Boolean, default=False, nullable=False)
    is_locked = Column(Boolean, default=False, nullable=False)


class User(Base):
    __tablename__ = "pks_user_user"
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Other fields


class UserWallet(Base):
    __tablename__ = "pks_market_wallet"
    id = Column(Integer, primary_key=True)

    user_id = Column(Integer, ForeignKey("pks_user_user.id"), nullable=False)
    usdt_balance = Column("balance", Numeric(precision=10, scale=2))


# Rate limiters
tron_api_limiter = AsyncLimiter(100, 1)
db_limiter = AsyncLimiter(1000, 1)


async def get_tron_balance(address: str) -> Tuple[Decimal, Decimal]:
    async with tron_api_limiter:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{TRON_API_URL}/v1/accounts/{address}"
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    trx_balance = Decimal(data.get("balance", 0)) / Decimal(
                        1_000_000
                    )
                    usdt_balance = Decimal(0)
                    for token in data.get("trc20", []):
                        if token[USDT_CONTRACT_ADDRESS]:
                            usdt_balance = Decimal(
                                token[USDT_CONTRACT_ADDRESS]
                            ) / Decimal(1_000_000)
                            break
                    return trx_balance, usdt_balance
                else:
                    raise ValueError(
                        f"Failed to get balance for address {address}"
                    )


async def send_tron_transaction(
    from_address: str,
    to_address: str,
    amount: Decimal,
    token_address: str = None,
) -> bool:
    async with tron_api_limiter:
        async with aiohttp.ClientSession() as session:
            if token_address:
                endpoint = f"{TRON_API_URL}/wallet/triggersmartcontract"
                data = {
                    "owner_address": from_address,
                    "contract_address": token_address,
                    "function_selector": "transfer(address,uint256)",
                    "parameter": f"{to_address},{int(amount * Decimal(1_000_000))}",
                    "fee_limit": FEE_LIMIT,
                }
            else:
                endpoint = f"{TRON_API_URL}/wallet/createtransaction"
                data = {
                    "to_address": to_address,
                    "owner_address": from_address,
                    "amount": int(amount * Decimal(1_000_000)),
                }

            async with session.post(endpoint, json=data) as response:
                return response.status == 200


@activity.defn
async def check_user_balance_and_withdraw(params: dict) -> bool:
    async with db_limiter:
        async with async_session() as session:
            async with session.begin():
                user_wallet = await session.execute(
                    select(UserWallet)
                    .where(UserWallet.user_id == params["user_id"])
                    .with_for_update()
                )
                user_wallet = user_wallet.scalar_one_or_none()

                if (
                    not user_wallet
                    or user_wallet.usdt_balance < params["amount"]
                ):
                    raise ValueError(
                        f"Insufficient balance for user {params['user_id']}"
                    )

                user_wallet.usdt_balance -= params["amount"]
                await session.commit()

    return True


@activity.defn
async def rollback_user_balance(params: Dict) -> bool:
    async with db_limiter:
        async with async_session() as session:
            async with session.begin():
                user_wallet = await session.execute(
                    select(UserWallet)
                    .where(UserWallet.user_id == params["user_id"])
                    .with_for_update()
                )
                user_wallet = user_wallet.scalar_one_or_none()

                if not user_wallet:
                    raise ValueError(
                        f"User wallet not found for user {params['user_id']}"
                    )

                user_wallet.usdt_balance += params["amount"]
                await session.commit()

    return True

@activity.defn
async def select_addresses_for_withdrawal(
    params: Dict,
) -> Tuple[str, List[Dict]]:
    async with db_limiter:
        async with async_session() as session:
            selected = []
            remaining = params["target_amount"]
            consolidation_address = None

            while remaining > 0:
                async with session.begin():
                    query = (
                        select(InnerAddress)
                        .where(
                            InnerAddress.usdt_balance > 0,
                            InnerAddress.is_locked == False,
                        )
                        .order_by(desc(InnerAddress.usdt_balance))
                        .with_for_update(skip_locked=True)
                        .limit(1)
                    )
                    result = await session.execute(query)
                    address = result.scalar_one_or_none()

                    if not address:
                        break

                    if not consolidation_address:
                        consolidation_address = address.address
                    elif address.address != consolidation_address:
                        if address.usdt_balance <= remaining:
                            selected.append(
                                {
                                    "address": address.address,
                                    "amount": address.usdt_balance,
                                }
                            )
                            remaining -= address.usdt_balance
                        else:
                            selected.append(
                                {
                                    "address": address.address,
                                    "amount": remaining,
                                }
                            )
                            remaining = 0

                    address.is_locked = True
                    await session.commit()

            if remaining > 0:
                raise ValueError(
                    f"Insufficient USDT funds across addresses. Short by {remaining}"
                )

            return consolidation_address, selected



@activity.defn
async def withdraw_usdt_to_external(params: Dict) -> bool:
    try:
        gas_replenished = await check_and_replenish_gas(
            {"address": params["from_address"], "amount": params["amount"]}
        )
        if not gas_replenished:
            return False

        success = await send_tron_transaction(
            params["from_address"],
            params["to_address"],
            params["amount"],
            USDT_CONTRACT_ADDRESS,
        )

        if success:
            async with db_limiter:
                async with async_session() as session:
                    async with session.begin():
                        query = (
                            update(InnerAddress)
                            .where(
                                InnerAddress.address == params["from_address"]
                            )
                            .values(
                                usdt_balance=InnerAddress.usdt_balance
                                - params["amount"]
                            )
                        )
                        await session.execute(query)
                    await session.commit()

            return True
        else:
            return False
    except Exception as e:
        raise ApplicationError(
            f"Error during USDT withdrawal to external address: {str(e)}"
        )


@activity.defn
async def unlock_addresses(params: Dict) -> None:
    async with db_limiter:
        async with async_session() as session:
            async with session.begin():
                query = (
                    update(InnerAddress)
                    .where(InnerAddress.address.in_(params["addresses"]))
                    .values(is_locked=False)
                )
                await session.execute(query)
            await session.commit()



# ====== woker.py ======
import asyncio
from temporalio.worker import Worker
from utils import setup_temporal_client
# Import the workflow and activities from your main file
from crypto_withdrawal_workflow import (
    CryptoWithdrawalWorkflow,
    check_user_balance_and_withdraw,
    rollback_user_balance,
    check_and_replenish_gas,
    select_addresses_for_withdrawal,
    internal_usdt_transfer,
    withdraw_usdt_to_external,
    unlock_addresses,
    send_notification,
)

async def run_worker():
    client = await setup_temporal_client()

    worker = Worker(
        client,
        task_queue="withdrawal-task-queue",
        workflows=[CryptoWithdrawalWorkflow],
        activities=[
            check_user_balance_and_withdraw,
            rollback_user_balance,
            check_and_replenish_gas,
            select_addresses_for_withdrawal,
            internal_usdt_transfer,
            withdraw_usdt_to_external,
            unlock_addresses,
            send_notification,
        ]
    )

    await worker.run()

if __name__ == "__main__":
    asyncio.run(run_worker())
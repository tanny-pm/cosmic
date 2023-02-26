import abc

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

import config
from adapters import repository


class AbstractUnitOfWork(abc.ABC):
    products: repository.AbstractProductRepository

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.rollback()

    @abc.abstractmethod
    def commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self):
        raise NotImplementedError


DEFAULT_SESSION_FACTORY = sessionmaker(
    bind=create_engine(
        config.get_postgres_uri(),
        isolation_level="SERIALIZABLE",
    )
)


class AutoRollbackUoW:
    def __init__(self, uow: AbstractUnitOfWork):
        self._uow = uow
        self.products = self._uow.products

    def __enter__(self):
        return self._uow.__enter__()

    def __exit__(self, *args):
        self._uow.rollback()
        return self._uow.__exit__(*args)

    def __getattr__(self, name):
        return getattr(self._uow, name)


class EventPublishingUoW:
    def __init__(self, uow: AutoRollbackUoW):
        self._uow = uow
        self.products = self._uow.products

    def commit(self):
        self._uow.commit()
        self.publish_events()

    def publish_events(self):
        for product in self.products.seen:
            while product.events:
                event = product.events.pop(0)
                # messagebus.handle(event)

    def __enter__(self):
        return self._uow.__enter__()

    def __exit__(self, *args):
        return self._uow.__exit__(*args)

    def __getattr__(self, name):
        return getattr(self._uow, name)


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    def __init__(self, session_factory=DEFAULT_SESSION_FACTORY):
        self.session_factory = session_factory

    def __enter__(self):
        self.session: Session = self.session_factory()
        self.products = repository.SqlAlchemyRepository(self.session)
        return super().__enter__()

    def __exit__(self, exn_type, exn_value, traceback):
        if exn_type is None:
            self.commit()
        else:
            self.rollback()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

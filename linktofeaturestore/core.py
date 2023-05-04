import contextlib
import sqlalchemy.orm
import sqlalchemy as sqla
from sqlalchemy import update
import datetime as dt

from mlopskit.pastry.data_store import FeatureSet
from mlopskit import make

Base = sqlalchemy.orm.declarative_base()


class LinkToFeatureStore:
    def __init__(self, name="feature_store", version="1", url=None):
        db_file = make("client").build_data_store(
            name, version, db_type="sqlite", return_type="dbfile"
        )
        if url is None:
            self.engine = sqla.create_engine(db_file)
        else:
            self.engine = sqla.create_engine(url)

        self.feature_db = make(f"db/{name}-v{version}", db_type="sqlite")
        self.build()

    def build(self):
        Base.metadata.create_all(self.engine)

    @contextlib.contextmanager
    def session(self):
        session_maker = sqlalchemy.orm.sessionmaker(self.engine)
        try:
            with session_maker.begin() as session:
                yield session
        finally:
            pass

    def add(self, loop, kind="feature_set"):
        self.feature_db.store(loop, kind)
        return True

    def update(self, uid, **kwargs):
        with self.session() as session:
            # session.update(FeatureSet).where(FeatureSet.uid == uid).values(**kwargs)
            created_at = dt.datetime.utcnow()
            kwargs["created_at"]=created_at
            stmt = update(FeatureSet).where(FeatureSet.uid == uid).values(**kwargs)
            session.execute(stmt)
            session.commit()

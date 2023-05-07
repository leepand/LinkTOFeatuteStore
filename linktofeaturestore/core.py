import contextlib
import sqlalchemy.orm
import sqlalchemy as sqla
from sqlalchemy import update, delete, and_
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
        self.feature_cache = make(
            f"cache/{name}-v{version}", db_name="feature_cache.db"
        )
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

    def add(self, loop, kind="feature_set", uniadd=True):
        uid = loop.uid
        name = loop.name
        version = loop.version
        uni_key = f"{uid}:{name}:{version}"
        if uniadd:
            uni_saved = self.feature_cache.get(uni_key)
            if uni_saved:
                feature_type = loop.feature_type
                cn_name = loop.cn_name
                log_type = loop.log_type
                author = loop.author
                status = loop.status
                content = loop.content
                self.update_mutil(
                    uid=uid,
                    name=name,
                    version=version,
                    feature_type=feature_type,
                    cn_name=cn_name,
                    log_type=log_type,
                    author=author,
                    status=status,
                    content=content,
                )
            else:
                self.feature_db.store(loop, kind)
                self.feature_cache.set(uni_key, "1")
        else:
            self.feature_db.store(loop, kind)
        return True

    def get(self, kind="feature_set", **kwargs):
        klass = {"feature_set": FeatureSet}[kind]
        with self.session() as session:
            # return session.query(klass).filter_by(uid=str(uid)).first().to_dataclass()
            xs = session.query(klass).filter_by(**kwargs)
            result = [o.to_dataclass() for o in xs.all()]
            # [vars(x) for x in result]
            return result

    def update_mutil(self, uid, name, version, **kwargs):
        with self.session() as session:
            # session.update(FeatureSet).where(FeatureSet.uid == uid).values(**kwargs)
            created_at = dt.datetime.utcnow()
            kwargs["created_at"] = created_at
            stmt = (
                update(FeatureSet)
                .where(
                    FeatureSet.uid == uid,
                    FeatureSet.name == name,
                    FeatureSet.version == version,
                )
                .values(**kwargs)
            )
            session.execute(stmt)
            session.commit()

    def update(self, uid, **kwargs):
        with self.session() as session:
            # session.update(FeatureSet).where(FeatureSet.uid == uid).values(**kwargs)
            created_at = dt.datetime.utcnow()
            kwargs["created_at"] = created_at
            stmt = update(FeatureSet).where(FeatureSet.uid == uid).values(**kwargs)
            session.execute(stmt)
            session.commit()

    def delete(
        self,
        uid=None,
        name=None,
        cn_name=None,
        version=None,
        author=None,
        log_type=None,
        feature_type=None,
    ):
        conditions = []
        if uid:
            conditions.append(FeatureSet.uid == str(uid))
        if name:
            conditions.append(FeatureSet.name == str(name))
        if cn_name:
            conditions.append(FeatureSet.cn_name == str(cn_name))

        if version:
            conditions.append(FeatureSet.version == str(version))

        if author:
            conditions.append(FeatureSet.author == str(author))

        if log_type:
            conditions.append(FeatureSet.log_type == str(log_type))

        if feature_type:
            conditions.append(FeatureSet.feature_type == str(feature_type))
        if len(conditions) < 1:
            return "请指定要删除的条件"
        if uid and name and version:
            uni_key = f"{uid}:{name}:{version}"
            self.feature_cache.delete(uni_key)

        with self.session() as session:
            delete_query = delete(FeatureSet).where(and_(*conditions))
            session.execute(delete_query)
            session.commit()

from linktofeaturestore.core import LinkToFeatureStore
from mlopskit import FeatureSet

test = LinkToFeatureStore()

feature = FeatureSet(
    uid="21046110",
    name="last_pay",
    cn_name="上次付费",
    author="sundi",
    status="running",
    feature_type="float",
    log_type="feature",
    content="1.99",
    version="1",
)

# 当key=f"{uid}-{name}-{version}""存在时update，否则 insert
test.add(feature)
# 多条件查询
test.get(uid="21046110", author="sundi", version="1")
# test.update(uid="21046110", content="5.99")

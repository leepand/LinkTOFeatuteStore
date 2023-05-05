from linktofeaturestore.core import LinkToFeatureStore
from mlopskit import FeatureSet

test = LinkToFeatureStore()

feature = FeatureSet(
    uid="21046110",
    name="last_pay",
    cn_name="上次付费",
    author="孙迪",
    status="running",
    feature_type="float",
    log_type="feature",
    content="1.99",
    version="1",
)

test.add(feature)

# test.update(uid="21046110", content="5.99")

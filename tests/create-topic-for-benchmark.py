from diaspora_stream.api import Driver

driver = Driver(backend="diaspora_pfs", options={})
driver.create_topic("my_topic", options={"partitions":1})

from openhouse.dataloader import DataLoaderContext, OpenHouseDataLoader, __version__
from openhouse.dataloader.data_loader_split import DataLoaderSplit


def test_package_imports():
    """Test that package imports work correctly"""
    assert OpenHouseDataLoader is not None
    assert DataLoaderContext is not None
    assert DataLoaderSplit is not None
    assert isinstance(__version__, str)
    assert len(__version__) > 0


def test_data_loader():
    """Test placeholder until real tests are added"""
    pass

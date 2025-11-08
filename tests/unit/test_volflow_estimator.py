import numpy as np
from services.volflow_estimator.my_main import VolflowCalculator
from shared.config import load_config

config = load_config()

def test_calculate_volatility_basic():
    calc = VolflowCalculator(config)
    calc.price_history.extend([
        (0.0, 100.0),
        (1000.0, 100.5),   # Δt = 1 sec
        (2000.0, 100.7),
    ])

    vol = calc._calculate_variance_absolute()
    print("###############################",vol)
    tolerance = abs(vol-0.145)/0.145
    assert vol > 0
    assert np.isclose(vol, 0.145, rtol=0.05)
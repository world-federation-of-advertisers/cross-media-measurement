# Noise Removal via Quadratic Programming with Set Relationships

This project implements a quadratic programming (QP) approach for noise removal in data, specifically designed for cases where measurements relate to sets and their subsets or covers. It leverages the `qpsolvers` library to efficiently solve the optimization problem.

## Key Classes 

- **`Measurement`:**
   - Represents a single measurement with a mean value (`value`) and a standard deviation (`sigma`).

- **`SetMeasurementsSpec`:**
   - Stores information about the relationships between sets and their measurements.
   - Tracks subset relationships (`subsets_by_set`), cover relationships (`covers_by_set`), and the measurements associated with each set (`measurements_by_set`).
   - Provides methods to add these relationships and measurements incrementally.

- **`Solver`:**
   - Provides the (`solve`) method which takes in a (`SetMeasurementSpec`) and translates it to a quadratic program, and returns the solution.


## How It Works

1. **Data Representation:**
   - Create a `SetMeasurementsSpec` object.
   - Use the `add_subset_relation`, `add_cover`, and `add_measurement` methods to populate it with your data and the relationships between your sets.
   - Use the (`Solver.solve`) method to remove inconsistencies in the measurements in (`SetMeasurementSpec`)

2. **Constraint Generation:**
   - The code automatically generates inequality and equality constraints based on the defined relationships.
   - **Subset Constraints:** The sum of measurements in a subset must be less than or equal to the measurement of the parent set.
   - **Cover Constraints:** The sum of measurements in the sets of a cover must be greater than or equal to the measurement of the parent set.
   - **Zero-Variance Constraints:** Measurements with zero variance are treated as exact values, and equality constraints are added accordingly.

3. **Quadratic Programming:**
   - The objective function is the variance-adjusted L2 norm, which acts as a maximum likelihood estimate under the assumption of Gaussian noise.
   - The `qpsolvers` library is used to solve the QP problem, finding the optimal solution that minimizes inconsistencies while respecting the constraints.



## Prepare the environment, install dependencies and run correction on an Origin Report

```
git clone https://github.com/world-federation-of-advertisers/experimental.git
cd experimental
python3 -m venv ../noisecorrectionenv
source ../noisecorrectionenv/bin/activate
cd dp_consistency
pip3 install -r requirements.txt
python3 -m src.tools.correctoriginreport --path_to_report=/path/to/Origin_Report.xlsx --unnoised_edps "Linear TV"
```
This will correct the report and create a corrected a file called `Origin_Report_corrected.xlsx` in this folder. 

## Usage Example

```python
from collections import defaultdict
# ... (Your classes: Measurement, SetMeasurementsSpec)
from qpsolvers import solve_qp
import numpy as np


# ... (Load or create your data)

# Create a SetMeasurementsSpec and populate it
spec = SetMeasurementsSpec()
spec.add_subset_relation(1, 2)  # Set 2 is a subset of set 1
spec.add_cover(3, [4, 5])       # Sets 4 and 5 form a cover of set 3
# ... (Add measurements for each set)

# Formulate and solve the QP problem
# ... (Similar to the previous example, but use the data from spec)

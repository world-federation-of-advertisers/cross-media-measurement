# Quadratic Programming for Consistency Enforcement in Noisy Data

This project provides a Python implementation of a quadratic programming (QP) approach to address inconsistencies that
arise in data due to noise. The method focuses on ensuring consistency between measurement results, particularly when
dealing with sets and their subsets or unions.

## Core Idea

The central idea is to formulate the problem of inconsistency removal as a QP optimization problem. The objective is to
minimize the variance-adjusted L2 norm between the original (noisy) measurements and the adjusted estimates. This
adjustment process aims to find the most likely set of consistent values, given the observed noisy measurements and
their inherent uncertainties (variances).

## Mathematical Formulation

### Objective Function

The objective function to be minimized is:

$\|{ \dfrac{Y_i - \mu_{M_j}}{\sigma_{M_j}} }\|_2$

where:

* $Y_i$: Adjusted estimates for each set
* $M_j$: Original (noisy) measurements, note each set could have been measured more than once.
* $\sigma_{M_j}$: Standard deviations of the measurements

### Constraints

The optimization is subject to several constraints:

1. **Subset Constraints:** The measurements for subsets within a set must be less than or equal to the
   measurement of the parent set.
2. **Cover Constraints:** The sum of measurements for sets forming a cover must be greater than or equal to the
   measurement of the parent set they cover.
3. **Non-negativity:** All adjusted estimates (Y_i) must be greater than or equal to zero.
4. **Population Bound:** All adjusted estimates (Y_i) must be less than or equal to the total population (P).
5. **Zero-Variance Equality:** Measurements with zero variance are considered exact and are enforced as equality
   constraints (Y_i = M_j). Note: This is useful if one of the data sources does not have noise added.

## Interpretation

* The variance adjustment in the objective function allows for larger changes to measurements with higher variance (
  greater uncertainty) and encourages smaller changes to measurements with lower variance.
* Under the assumption of normally distributed measurement noise, this approach is equivalent to finding the maximum
  likelihood estimate (MLE) of the true values, meaning it selects the most likely consistent solution given the
  observed data and their uncertainties.

## Application to Origin's report

### Measurements ###

Origin baseline measurements are either direct (single EDP measurements), or union measurements for more than one EDP.
Each measurement is also associated to a time period, and a metric (e.g. mrc, ami, etc).

To compute unique reach for each EDP (when the number of EDPs are larger than 2), we must also compute the union of all
EDPs but one, for each EDP, for each measurement period and metric.

For TV measurements we always set the variance to 0, to indicate that TV measurements do not have DP noise.

### Cover Relationships ###

For each metric and period, it identifies the set representing the union of all entities/dimensions (EDPs) and the
individual sets for each EDP and add a "cover" relationship to the spec, indicating that the union set's measurement 
should be greater than or equal to the sum of the measurements of its constituent EDP sets.

We do the same for the unions computed for unique reach purposes. Also including the relationship that the
of all-but-edp-x set and the set x are a cover of the union of all EDPs.

### Subset Relationships ###

For each metric, period, and EDP, it establishes a subset relationship between the set representing that specific EDP
and the union set for that metric and period, for all unions that include that EDP for tha metric and period.

If there's a predefined hierarchy among metrics (where some metrics are inherently greater than or equal to others), it
enforces this relationship for each period and EDP, including the measured unions. For example: $MRC \lte AMI$.

It asserts the cumulative reach measurements in one period are less than or equal to those in the
subsequent period. It adds subset relationships to reflect this for each metric and EDP, including the union set.

## Implementation

The methodology is implemented using Python and the [qpsolvers](https://github.com/qpsolvers/qpsolvers)
 library to solve the QP problem. QP solvers offers a common interface over many QP implementations which
can make it easy to adjust underlying the solver engine over time.

The `SetMeasurementsSpec` class in the [noised_measurements](src/noiseninja/noised_measurements.py) package is used to
efficiently store and manage the relationships between sets and their
measurements, facilitating the automatic generation of constraints for the optimization.

## Experimental Results

Simulation results demonstrate a reduction in variance across all variables in the report, indicating improved
consistency after applying this method. More details will be added soon.

## Key Advantages

* **Statistically Sound:** Based on MLE, providing a principled approach to handling noisy data.
* **Flexible:** Can accommodate various types of constraints and relationships between sets.
* **Efficient:** Leverages state of the art QP solvers for fast and accurate solutions.

## Key Classes

- **`Measurement`:**
    - Represents a single measurement with a mean value (`value`) and a standard deviation (`sigma`).

- **`SetMeasurementsSpec`:**
    - Stores information about the relationships between sets and their measurements.
    - Tracks subset relationships (`subsets_by_set`), cover relationships (`covers_by_set`), and the measurements
      associated with each set (`measurements_by_set`).
    - Provides methods to add these relationships and measurements incrementally.

- **`Solver`:**
    - Takes in a (`SetMeasurementSpec`) and translates it to a quadratic program, and has methods to run QP and return
      the solution

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

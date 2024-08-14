from unittest import TestCase
from src.tools.correctoriginreport import correctExcelFile


CUML_REACH_COL_NAME = "Cumulative Reach 1+"
TOTAL_REACH_COL_NAME = "Total Reach (1+)"
FILTER_COL_NAME = "Impression Filter"

AMI_FILTER = "AMI"
MRC_FILTER = "MRC"


class TestOriginSheetReport(TestCase):
    def test_get_origin_report_corrected_successfully(self):
        correctedExcel = correctExcelFile(
            "tests/tools/example_origin_report.xlsx", "Linear TV"
        )
        (google_ami_rows, google_mrc_rows) = self.get_edp_rows(correctedExcel, "Google")
        (tv_ami_rows, tv_mrc_rows) = self.get_edp_rows(correctedExcel, "Linear TV")
        (total_ami_rows, total_mrc_rows) = self.get_edp_rows(
            correctedExcel, "Total Campaign"
        )

        # Ensure that subset relations are correct by checking larger time periods have more reach than smaller ones.
        self.__assert_is_monotonically_increasing(google_ami_rows)
        self.__assert_is_monotonically_increasing(google_mrc_rows)

        self.__assert_is_monotonically_increasing(tv_ami_rows)
        self.__assert_is_monotonically_increasing(tv_mrc_rows)

        self.__assert_is_monotonically_increasing(total_ami_rows)
        self.__assert_is_monotonically_increasing(total_mrc_rows)

        # Ensure that cover relations are correct by checking the sum of EDPs have larger reach than the total reach.
        self.__assert_pairwise_sum_greater(google_ami_rows, tv_ami_rows, total_ami_rows)
        self.__assert_pairwise_sum_greater(google_mrc_rows, tv_mrc_rows, total_mrc_rows)

        # Ensure that metric subset relation is correct by checking AMI is always larger than MRC.
        self.__assert_pairwise_greater(google_ami_rows, google_mrc_rows)
        self.__assert_pairwise_greater(tv_ami_rows, tv_mrc_rows)
        self.__assert_pairwise_greater(total_ami_rows, total_mrc_rows)

    def get_edp_rows(self, df, edp):
        tot_sheet = df[edp]
        cum_sheet = df[f"Cuml. Reach ({edp})"]
        ami_rows = (
            cum_sheet[cum_sheet[FILTER_COL_NAME] == AMI_FILTER][
                CUML_REACH_COL_NAME
            ].tolist()
            + tot_sheet[tot_sheet[FILTER_COL_NAME] == AMI_FILTER][
                TOTAL_REACH_COL_NAME
            ].tolist()
        )
        mrc_rows = (
            cum_sheet[cum_sheet[FILTER_COL_NAME] == MRC_FILTER][
                CUML_REACH_COL_NAME
            ].tolist()
            + tot_sheet[tot_sheet[FILTER_COL_NAME] == MRC_FILTER][
                TOTAL_REACH_COL_NAME
            ].tolist()
        )
        return (ami_rows, mrc_rows)

    def __assert_is_monotonically_increasing(self, lst):
        """Checks if a list is monotonically increasing."""
        self.assertTrue(all(lst[i] <= lst[i + 1] for i in range(len(lst) - 1)))

    def __assert_pairwise_sum_greater(self, list1, list2, list3):
        """Checks if the pairwise sum of the first two lists is pairwise greater than the third list."""
        if len(list1) != len(list2) or len(list1) != len(list3):
            raise ValueError("Lists must have the same length")
        self.assertTrue(all(list1[i] + list2[i] >= list3[i] for i in range(len(list1))))

    def __assert_pairwise_greater(self, list1, list2):
        """Checks if the first list is pairwise greater than the second list."""
        if len(list1) != len(list2):
            raise ValueError("Lists must have the same length")
        self.assertTrue(all(list1[i] >= list2[i] for i in range(len(list1))))

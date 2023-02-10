import subprocess
import os
from time import sleep


###################
# Benchmark setup #
###################

EDPS = ["edp1", "edp2", "edp3", "edp4", "edp5", "edp6"]

def benchmark_setup():
    print("=========================== Benchmark Setup ===========================")

    GET_HALO_MC_COMMAND = "kubectl logs jobs/resource-setup-job -c resource-setup-container | grep 'Successfully created measurement consumer' | awk '{print $6;}' | head -1"
    HALO_MC = subprocess.run(
        ["bash", "-c", GET_HALO_MC_COMMAND],
        capture_output=True, 
        encoding="utf-8"
    ).stdout.strip("\n")
    print(f"HALO_MC={HALO_MC}\n")

    GET_HALO_MC_APIKEY_COMMAND = "kubectl logs jobs/resource-setup-job -c resource-setup-container | grep 'API key for measurement consumer' | awk '{print $8;}'"
    HALO_MC_APIKEY = subprocess.run(
        ["bash", "-c", GET_HALO_MC_APIKEY_COMMAND],
        capture_output=True, 
        encoding="utf-8"
    ).stdout.strip("\n")
    print(f"HALO_MC_APIKEY={HALO_MC_APIKEY}\n")

    EVENT_GROUPS=[]
    DATA_PROVIDERS=[]

    for edp_index, EDP in enumerate(EDPS, 1):
        E = subprocess.run(
                ["bash", "-c", f"kubectl get pods | grep {EDP}" + "-simulator | awk '{print $1}'"],
                capture_output=True, 
                encoding="utf-8"
            ).stdout.strip("\n")

        EG_command = f"kubectl logs {E} -c edp{edp_index}-simulator-container" + " | grep 'Successfully created eventGroup' | awk '{print $5}' | tr -d ."
        EG = subprocess.run(
                ["bash", "-c", EG_command],
                capture_output=True, 
                encoding="utf-8"
            ).stdout.strip("\n")
        
        DP_command = f"echo {EG} | sed 's@/eventGroups.*@@'"
        DP = subprocess.run(
                ["bash", "-c", DP_command],
                capture_output=True, 
                encoding="utf-8"
            ).stdout.strip("\n")

        EVENT_GROUPS.append(EG)
        DATA_PROVIDERS.append(DP)

    print(f"EVENT_GROUPS={EVENT_GROUPS}\n")
    print(f"DATA_PROVIDERS={DATA_PROVIDERS}\n")

    return HALO_MC, HALO_MC_APIKEY, EVENT_GROUPS, DATA_PROVIDERS


HALO_MC, HALO_MC_APIKEY, EVENT_GROUPS, DATA_PROVIDERS = benchmark_setup()


#######################
# Benchmark execution #
#######################

# Variable setup
DEPLOYMENT_SCRIPT_PATH = "./kind_halo_launcher.sh"

BENCHMARK_BAZEL_TARGET = "src/main/kotlin/org/wfanet/measurement/api/v2alpha/tools:Benchmark"

OUTPUT_DIR = "/usr/local/google/home/riemanli/Data/benchmarking/outputs"

NUM_PUBLISHERS = 1
NUM_DEMOGRAPHY_TYPES = 1
DEMOGRAPHY_TYPES = [f".{i + 1}" for i in range(NUM_DEMOGRAPHY_TYPES)]

TIME_OUT = 10000
SLEEP_TIME_OUT = 1200

EXECUTION_TIME_OUT = f"--timeout={TIME_OUT}"

METRIC_DP_SETUP_DICT = {
    "RO": '--reach-privacy-epsilon=0.0041 \
--reach-privacy-delta=0.0000000001 \
--frequency-privacy-epsilon=1.0 \
--frequency-privacy-delta=0.1 \
--vid-sampling-start=0.0 \
--vid-sampling-width=0.01 \
--vid-bucket-count=50 \
--max-frequency-for-reach=1',
    "RF": '--reach-privacy-epsilon=0.0033 \
--reach-privacy-delta=0.0000000001 \
--frequency-privacy-epsilon=0.115 \
--frequency-privacy-delta=0.0000000001 \
--vid-sampling-start=0.16 \
--vid-sampling-width=0.016667 \
--vid-bucket-count=50 \
--max-frequency-for-reach=10',
}

EVENT_FILTER_DICT = {
    ".1": "video_ad.gender.value == 1 && video_ad.age.value == 1",
    ".2": "video_ad.age.value == 1",
    ".3": "video_ad.gender.value == 1",
    ".4": "video_ad.gender.value in [0,1,2]",
}

EVENT_FILTER_DICT = {demo_type: EVENT_FILTER_DICT[demo_type] for demo_type in DEMOGRAPHY_TYPES}

assert len(EVENT_FILTER_DICT) == NUM_DEMOGRAPHY_TYPES, "Number of event filters is not equal to number of demography types."


# Helping functions
def build_execution_const(HALO_MC, HALO_MC_APIKEY):
    return f'bazel-bin/src/main/kotlin/org/wfanet/measurement/api/v2alpha/tools/Benchmark \
  --tls-cert-file src/main/k8s/testing/secretfiles/mc_tls.pem \
  --tls-key-file src/main/k8s/testing/secretfiles/mc_tls.key \
  --cert-collection-file src/main/k8s/testing/secretfiles/kingdom_root.pem \
  --kingdom-public-api-target \
  localhost:8443 \
  --api-key {HALO_MC_APIKEY} \
  --measurement-consumer {HALO_MC} \
  --private-key-der-file=src/main/k8s/testing/secretfiles/mc_cs_private.der \
  --encryption-private-key-file=src/main/k8s/testing/secretfiles/mc_enc_private.tink \
  --reach-and-frequency \
  --repetition-count=20'

def build_output_path(number_publisher, demography_type, metric_type):
    return f"{OUTPUT_DIR}/benchmark-results-A{metric_type}-{number_publisher}{demography_type}-kind.csv"

def build_execution_command(number_publisher, demography_type, metric_type, execution_const):
    execution_metric_dp_setup = METRIC_DP_SETUP_DICT[metric_type]
    output_path = build_output_path(number_publisher, demography_type, metric_type)
    execution_output = f'--output-file="{output_path}"'

    execution_event_specs = " ".join([
            f'--data-provider "{DATA_PROVIDERS[i]}" --event-group "{EVENT_GROUPS[i]}" '
            '--event-end-time=2022-05-24T05:00:00.000Z --event-start-time=2022-05-22T01:00:00.000Z '
            f'--event-filter="{EVENT_FILTER_DICT[demography_type]}"'
            for i in range(number_publisher)
        ]
    )

    return " ".join(
        [
            execution_const, 
            EXECUTION_TIME_OUT,
            execution_output,
            execution_metric_dp_setup,
            execution_event_specs,
        ]
    )

def wait_epds(edps):
    print("\n\nWait until the EDP simulators are ready.\n\n")
    pod_processes = [
        subprocess.run(
            ["bash", "-c", f"kubectl get pods | grep {edp}-simulator"], 
            capture_output=True, 
            encoding="utf-8"
        )
        for edp in edps
    ]

    return_codes = [process.returncode for process in pod_processes]
    grep_outputs = [process.stdout for process in pod_processes]

    if any(return_codes) or not all(grep_outputs):
        raise RuntimeError("Not all EDP simulators got deployed.")

    edp_pod_names = [grep_output.split()[0] for grep_output in grep_outputs]
    check_commands = [
        f'kubectl logs {edp_pod_name} -c edp{edp_index}-simulator-container'
        ' | grep "INFO: Executing requisitionFulfillingWorkflow..." | tail -1'
        for edp_index, edp_pod_name in enumerate(edp_pod_names, 1)
    ]
    edp_log_outputs = ["" for _ in edps]
    sleep_time = 10

    while not all(edp_log_outputs):
        print(f"Wait {sleep_time} secs...")
        sleep(sleep_time)
        if sleep_time > SLEEP_TIME_OUT:
            raise RuntimeError("Not all EDP simulators got deployed correctly.")
        sleep_time *= 2

        processes = [
            subprocess.run(["bash", "-c", command], capture_output=True, encoding="utf-8")
            for command in check_commands
        ]
        edp_log_outputs = [process.stdout for process in processes]

    print("\nEDP simulators are ready.\n")
    sleep(10)


# Execution
subprocess.run(["bash", "-c", f"bazel build {BENCHMARK_BAZEL_TARGET}"])

metric_type = "RO"

execution_const = build_execution_const(HALO_MC, HALO_MC_APIKEY)

for number_publisher in range(1, NUM_PUBLISHERS + 1):
    for demography_type in DEMOGRAPHY_TYPES:
        output_path = build_output_path(number_publisher, demography_type, metric_type)

        if os.path.exists(output_path):
            print(f"\n\n{output_path} already exists.\n\n")
            continue

        execution_command = build_execution_command(number_publisher, demography_type, metric_type, execution_const)
        print("Execute: ")
        print(execution_command,"\n\n\n")

        process = None

        try:
            process = subprocess.run(["bash", "-c", execution_command], timeout=TIME_OUT, check=True)

        except subprocess.TimeoutExpired as exc:
            print("\n\nProcess timed out. Redeploy the system and run it again.\n\n")
            subprocess.run(["bash", "-c", DEPLOYMENT_SCRIPT_PATH])

            wait_epds(EDPS[:number_publisher])
            HALO_MC, HALO_MC_APIKEY, EVENT_GROUPS, DATA_PROVIDERS = benchmark_setup()
            execution_const = build_execution_const(HALO_MC, HALO_MC_APIKEY)
            execution_command = build_execution_command(
                number_publisher, demography_type, metric_type, execution_const
            )

        except subprocess.CalledProcessError as exc:
            print("\n\nProcess error. Rerun the command.\n\n")
    
        sleep(0.1)
        
        if process is None or process.returncode:
            print("\nRetry...\n")
            subprocess.run(["bash", "-c", execution_command], timeout=TIME_OUT, check=True)

This code is to deply Duchy VM and install prerequisite packages also build Duchy Image 
Below are the steps to check the above details
1. run from local terraform plan '-out=qa_tfplan' && terraform apply qa_tfplan
2. wait for it to complete.
3. Open the VM from console and run tail -f /tmp/init_script.log  ctrl c.
4. tail -100f /tmp/build_image.log use the file name from 3rd line for the next step.
5. tail -f /root/.cache/bazel/_bazel_root/52c52bf7d1b34c870907a50f5116eb80/command.log
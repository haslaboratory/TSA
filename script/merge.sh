NUM=16
ZSIZE=0x400000

lba=0
for i in $(seq 1 $NUM); do
    sudo  nvme io-passthru /dev/nvme0n1 --namespace-id=1  --opcode=0x79 --cdw10=$lba --cdw12=0x2 --cdw13=0x6
    let lba=$lba+$ZSIZE
done
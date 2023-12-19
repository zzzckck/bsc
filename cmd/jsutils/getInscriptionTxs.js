import { ethers } from "ethers";
import program from "commander";

program.option("--rpc <rpc>", "Rpc");
program.option("--startNum <startNum>", "start num")
program.option("--endNum <endNum>", "end num")
program.parse(process.argv);

const provider = new ethers.JsonRpcProvider(program.rpc)


// eth.getTransactionsByBlockNumber("0x3BE")

// inscription 1:
// https://bscscan.com/tx/0x9168b9b633d34c301341380d0b763bd4e1228dcbaa67c647de08eeb8ed6b29f0
// 0x646174613a2c7b2270223a22 6273632d3230 222c226f70223a226d696e74222c227469636b223a2262736369222c22616d74223a2231303030227d
// data:,{"p":"bsc-20","op":"mint","tick":"bsci","amt":"1000"}
// bsc-20: 6273632d3230
const main = async () => {
    let bsc20Hex = "6273632d3230"
    let globalTxsNum = 0
    let globalFromTo = 0
    let globalFromToLength = 0
    let globalFromToLengthBsc20 = 0
    console.log("Find inscription txs count between", program.startNum, "and", program.endNum);
    for (let i = program.startNum; i < program.endNum; i++) {
        let txs = await provider.send("eth_getTransactionsByBlockNumber", [ethers.toQuantity(i)]);
        let txsNum = txs.length
        let fromTo = 0
        let fromToLength = 0
        let fromToLengthBsc20 = 0
        for (let j = 0; j < txsNum; j++) {
            let tx = txs[j]
            if (tx.from == tx.to) {
                fromTo++
                if (tx.input.length > 40) {
                    fromToLength++
                    if (tx.input.includes(bsc20Hex)) {
                        fromToLengthBsc20++
                    }
                }
            }
        }
        globalTxsNum += txsNum
        globalFromTo += fromTo
        globalFromToLength += fromToLength
        globalFromToLengthBsc20 += fromToLengthBsc20
        console.log("block", i, "txsNum", txsNum, "fromTo", fromTo, "fromToLength", fromToLength, "fromToLengthBsc20",fromToLengthBsc20);
        console.log("global per block, txs", (globalTxsNum)/(i-program.startNum+1),
                     "fromTo", (globalFromTo)/(i-program.startNum+1),
                     "fromToLength", (globalFromToLength)/(i-program.startNum+1),
                     "fromToLengthBsc20", (globalFromToLengthBsc20)/(i-program.startNum+1));
    }
    // console.log("done, fromTo", fromTo, "fromToLength", fromToLength, "fromToLengthBsc20",fromToLengthBsc20);
};

main().then(() => process.exit(0))
    .catch((error) => {
        console.error(error);
        process.exit(1);
    });
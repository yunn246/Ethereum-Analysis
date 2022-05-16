from mrjob.job import MRJob
from mrjob.step import MRStep

'''
Transactions
0 block_number: Block number where this transaction was in
1 from_address: Address of the sender
2 to_address: Address of the receiver. null when it is a contract creation transaction
3 value: Value transferred in Wei (the smallest denomination of ether)
4 gas: Gas provided by the sender
5 gas_price : Gas price provided by the sender in Wei
6 block_timestamp: Timestamp the associated block was registered at (effectively timestamp of the transaction)

Contracts
0 address: Address of the contract
1 is_erc20: Whether this contract is an ERC20 contract
2 is_erc721: Whether this contract is an ERC721 contract
3 block_number: Block number where this contract was created
4 block_timestamp
'''

class mrjob_partB(MRJob):
    def mapper1(self, _, line):
        fields=line.split(',')
        try:
            if len(fields) == 7:
                to_add = str(fields[2])
                value = float(fields[3])
                yield (to_add, (value,1))

            if len(fields) == 5:
                address = fields[0]
                yield (address,(None,2))

        except:
            pass

    def reducer1(self, address, values):
        value_list=[]
        # temporary variable to check if it is smart contract
        is_c = False
        for val in values:
            if val[1]==1: # transaction
                value_list.append(val[0])
            if val[1]==2: # contract
                is_c = True
        if is_c is True:
            total_values = sum(value_list)
            yield (address, total_values)

    def mapper2(self,address,values):
        yield (None, (address,values))

    def reducer2(self, _, pairs):
        sorted_values = sorted(pairs, reverse=True, key=lambda x:x[1])
        top_10 = sorted_values[:10]
        for val in top_10:
            yield (val[0], val[1])

    def steps(self):
        return [MRStep(mapper=self.mapper1,reducer=self.reducer1),
				MRStep(mapper=self.mapper2,reducer=self.reducer2)]

if __name__ == '__main__':
    mrjob_partB.run()




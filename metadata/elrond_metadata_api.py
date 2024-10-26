

# "operations_0.action",
# "operations_0.type",
# "operations_0.value",
# "operations_0.in_out",
# 'operations_0.ticker',
# "operations_1.action",
# "operations_1.type",
# "operations_1.ticker",
# "operations_1.value",
# "operations_1.in_out",

lista_colonne_tx_standard = [
'timestamp',
'action.name',
'type',
'fee',
'function',
'value',
'sender',
'receiver',
'ticker',
'txHash'
]

colonne_finali_interesse = [
    'timestamp',
    ''

]



lista_chiavi = [
"timestamp",
"action.name",
"fee",
"function",
"value",
"IN_OR_OUT",

"sender",
"receiver",
"txHash"
]


non_tener_conto = [
    'claimLaunchpadTokens', 'claimLockedAssets', 'claimRewards', 'compoundRewardsProxy'
    'delegate', 'enterFarmAndLockRewardsProxy', 'enterFarmAndLockRewards', 'exitFarmProxy',
    'stake', 'stakeFarm','unBond', 'unDelegate','unStake', 'ESDTNFTTransfer',
]
add_amount = [
    'ESDTTransfer', 'MultiESDTNFTTransfer', 'transfer', 'ESDTNFTTransfer', 'enterFarmAndLockRewards'
]

sub_amount = [
    'confirmTickets', 'buyTickets']

# Aggiungere la condizione che io devo essere il receiver
# Se non lo sono ma sono il sender allora levami quella roba

# Tutte le transazioni dove il sender == receiver levale





def return_deposit_transactions(df, action_col="Transaction Description", amount_col="Amount"):
    deposit_unique_list = df[df[action_col].str.contains(r'\bDeposit\b')][action_col][(df[amount_col] >= 0)].unique()
    return deposit_unique_list


def return_withdraw_transactions(df, action_col="Transaction Description"):
    withdraw_unique_list = df[df[action_col].str.contains(r'\bWithdraw\b')][action_col].unique()
    return withdraw_unique_list


def return_buy_transactions(df, action_col="Transaction Description"):
    buy_unique_list = df[df[action_col].str.contains(r'\bBuy \b')][action_col].unique()
    return buy_unique_list


def return_recurring_buy_transactions(df, action_col="Transaction Description"):
    buy_unique_list = df[df[action_col].str.contains(r'\bRecurring buy\b')][action_col].unique()
    return buy_unique_list


def return_transfer_transactions(df, action_col="Transaction Description"):
    transfer_unique_list = df[df[action_col].str.contains(r'Transfer: ')][action_col].unique()
    return transfer_unique_list


def return_exchange_transactions(df, action_col="Transaction Description"):
    exchange_unique_list = set(df[df[action_col].str.contains(r'->')][action_col].unique())
    transfer_unique_list = set(df[df[action_col].str.contains(r'Transfer: ')][action_col].unique())
    intersection = list(exchange_unique_list - transfer_unique_list)
    return intersection

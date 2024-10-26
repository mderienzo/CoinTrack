def return_reward_transactions(df, action_col="Transaction Description", amount_col="Amount"):
    reward_unique_list = df[df[action_col].str.contains(r'\bReward\b')][action_col].unique()
    rewards_unique_list = df[df[action_col].str.contains(r'\bRewards\b')][action_col].unique()

    return list(reward_unique_list) + list(rewards_unique_list)

add_amount = [
    'Card Cashback', 'Card Rebate: Netflix', 'Card Rebate: Spotify', 'Crypto Earn',
    'Sign-up Bonus Unlocked', 'DPoS non-compound interest received', 'DPoS compound interest received', 'Adjustment (Credit)'
]

in_dubbio = [
    'Adjustment (Credit)'
]
non_tener_conto = [
    'CRO Unlock', 'Crypto Earn Withdrawal', 'Supercharger Lockup (via app)', 'Crypto Earn Allocation',
    'Crypto Earn Deposit', 'Supercharger Withdrawal (via app)', 'Supercharger Deposit (via app)', 'CRO Lockup',
]


sub_amount = [
    'Card Cashback Reversal', 'Convert Dust', 'To +393388584434', 'Unstoppable Domains Inc.', 'MCO/CRO Overall Swap']


def return_deposit_transactions(df, action_col="Transaction Description", amount_col="Amount"):
    deposit_unique_list = df[df[action_col].str.contains(r'\bDeposit\b')][action_col][(df[amount_col] >= 0)].unique()
    return deposit_unique_list

deposit = [
    'ETH (ERC20) Deposit', 'BTC (BTC) Deposit', 'CRO (CRO) Deposit',
    'ADA Deposit', 'BTC Deposit', 'ETH Deposit',
]


def return_withdraw_transactions(df, action_col="Transaction Description"):
    withdraw_unique_list = df[df[action_col].str.contains(r'\bWithdraw\b')][action_col].unique()
    return withdraw_unique_list


withdraw = [
    'Withdraw ATOM', 'Withdraw BTC (BTC)', 'Withdraw ETH (ERC20)',
    'Withdraw KSM', 'Withdraw BNB', 'Withdraw ADA', 'Withdraw EGLD',
    'Withdraw EGLD (EGLD)', 'Withdraw DOT', 'Withdraw CRO',
    'Withdraw BTC', 'Withdraw CRO (CRO)'
]


def return_buy_transactions(df, action_col="Transaction Description"):
    buy_unique_list = df[df[action_col].str.contains(r'\bBuy \b')][action_col].unique()
    return buy_unique_list

def return_recurring_buy_transactions(df, action_col="Transaction Description"):
    buy_unique_list = df[df[action_col].str.contains(r'\bRecurring buy\b')][action_col].unique()
    return buy_unique_list

buy = ['Buy ONT',
       'Buy ICX', 'Buy NANO', 'Buy OGN', 'Buy REN', 'Buy SOL', 'Buy BAT',
       'Buy GLM', 'Buy EGLD', 'Buy AAVE', 'Buy SNX', 'Buy MKR', 'Buy DASH',
       'Buy XLM', 'Buy ADA', 'Buy ALGO', 'Buy XTZ', 'Buy LINK', 'Buy ATOM',
       'Buy BTC', 'Buy ETH', 'Buy DOT', 'Buy ZIL', 'Buy CRO']


def return_transfer_transactions(df, action_col="Transaction Description"):
    transfer_unique_list = df[df[action_col].str.contains(r'Transfer: ')][action_col].unique()
    return transfer_unique_list
def return_exchange_transactions(df, action_col="Transaction Description"):
    exchange_unique_list = set(df[df[action_col].str.contains(r'->')][action_col].unique())
    transfer_unique_list = set(df[df[action_col].str.contains(r'Transfer: ')][action_col].unique())
    intersection = list(exchange_unique_list - transfer_unique_list)
    return intersection


exchanges = [
    'CRO -> BTC',
    'CRO -> ADA', 'CRO -> ETH', 'ADA -> BTC', 'USDT -> EGLD',
    'BTC -> BNB', 'CRO -> EGLD', 'ETH -> CRO', 'CRO -> USDT',
    'CRO -> MATIC', 'ETH -> BTC', 'USDT -> ETH', 'USDT -> BTC',
    'USDT -> ADA', 'BTC -> USDT', 'BTC -> ETH', 'BTC -> ADA',
    'ETH -> USDT', 'ADA -> USDT', 'HOT -> USDT', 'RLY -> USDT',
    'THETA -> USDT', 'ENJ -> USDT', 'RVN -> USDT', 'OMG -> USDT',
    'KMD -> USDT', 'LSK -> USDT', 'BLZ -> USDT', 'REN -> USDT',
    'TFUEL -> USDT', 'CRV -> USDT', 'ONT -> USDT', 'ONE -> USDT',
    'NANO -> USDT', 'SNX -> USDT', 'ICX -> USDT', 'ALGO -> USDT',
    'SAND -> USDT', 'MANA -> USDT', 'FIL -> USDT', 'CHZ -> USDT',
    'GLM -> USDT', 'MKR -> USDT', 'XCH -> USDT', 'ZIL -> USDT',
    'DOGE -> USDT', 'BAT -> USDT', 'AAVE -> USDT', 'XLM -> USDT',
    'SOL -> USDT', 'DASH -> USDT', 'UNI -> USDT', 'SHIB -> USDT',
    'BNB -> USDT', 'EGLD -> USDT', 'CRO -> HOT', 'USDT -> XCH',
    'USDT -> DOGE', 'CRO -> SOL', 'CRO -> SHIB', 'CRO -> BNB',
    'BTC -> EGLD', 'BTC -> ONE', 'BTC -> OMG',
    'BTC -> KMD', 'BTC -> ICX', 'BTC -> RLY', 'BTC -> BLZ',
    'BTC -> LSK', 'BTC -> THETA', 'BTC -> CRV', 'BTC -> RVN',
    'SOL -> BTC', 'BTC -> CRO', 'SAND -> BTC',
    'BTC -> SOL', 'USDT -> SAND', 'USDT -> CHZ', 'USDT -> MANA',
    'USDT -> FIL', 'USDT -> CRO', 'BNB -> BTC', 'BTC -> BAT',
    'EUR -> EGLD', 'USDT -> KSM', 'USDT -> ENJ', 'EUR -> TFUEL',
    'CRO -> UNI', 'EUR -> BTC', 'BAT -> BTC', 'XLM -> BTC',
    'USDT -> BNB', 'LINK -> USDT', 'OGN -> BTC', 'XTZ -> BTC',
    'XRP -> BTC', 'BTC -> XRP', 'USDT -> AAVE', 'BTC -> XLM', 'CRO -> DOT', 'BTC -> DOT'
]

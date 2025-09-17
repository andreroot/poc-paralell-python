def insert_operacoes_df(engine, df):

    print('Insirting monthly guaranteed savings operation infos...')
        
    with engine.begin() as con:
        df.to_sql(
            con=con,
            schema='ThundersAPIGuaranteedSavingsContracts',
            name='monthly_view',
            if_exists='replace',
            index=False,
            chunksize=1000
        )

    print('Done inserting custo_marginal_hidrologico data on SQL Server.')
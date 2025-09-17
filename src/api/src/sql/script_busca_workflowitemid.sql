      with base as (
            select *,
            case
                when datediff(day, signatureDate, getdate()) >= 60 then 'S'
                when step = 'Contratos Passados' then 'S'
                when datediff(day, createdDate, getdate()) >= 60 and step = 'Cancelado' then 'S'
                when datediff(day, endDate, getdate()) >= 60 then 'S'
                else 'N'
            end as desconsiderar
            from apiworkflows.juridicUnion
			where workflowitemid = '5a9c1b36-256b-4c73-a22f-c4fabcd4a8cb'
        )
        select distinct workflowItemid
        from base
        where desconsiderar = 'N'
            and workflowItemid is not null
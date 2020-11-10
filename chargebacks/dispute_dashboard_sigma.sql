WITH disputes_won 
     AS (SELECT reason, 
                Count(id) AS disputes_won 
         FROM   disputes 
         WHERE  created > Now() - interval '6' month 
                AND status = 'won' 
         GROUP  BY 1), 
     disputes_data 
     AS (SELECT disputes.reason, 
                Count(disputes.id)                                       AS 
                disputes, 
                Count_if(disputes.evidence_details_submission_count > 0) AS 
                disputes_with_evidence_submitted 
         FROM   disputes 
                left join charges 
                       ON disputes.charge_id = charges.id 
         WHERE  disputes.created > Now() - interval '6' month 
         GROUP  BY 1 
         ORDER  BY 2 DESC) 
SELECT disputes_data.*, 
       disputes_won.disputes_won 
FROM   disputes_data 
       left join disputes_won 
              ON disputes_data.reason = disputes_won.reason 
ORDER  BY 2 DESC 
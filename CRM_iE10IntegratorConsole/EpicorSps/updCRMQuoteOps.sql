update [dbo].[epic6s_quotec2eBase]
Set epic6s_Processed = 1
where epic6s_Processed = 0 and epic6s_Reference = '#QUOTENUM#'
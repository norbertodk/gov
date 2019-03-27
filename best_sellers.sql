select BillingCountry, the_year, album, sales_qtty, dense_rank() over ( order by sales_qtty desc) rr
from (
	select t1.BillingCountry, t1.the_year, t2.album, 
	sum(t1.Quantity) sales_qtty
	from 
		(
		select i.BillingCountry, strftime('%Y', i.InvoiceDate) the_year,
		it.trackid,
		it.Quantity
		from invoices i left join invoice_items it on i.InvoiceId = it.InvoiceId
		where strftime('%Y',invoicedate) > '2012'
		and upper(i.BillingCountry) = upper('Argentina')
		) t1 
	join 
		(
		select t.trackid, t.albumid , a.title album
		from tracks t join genres g on t.genreid = g.genreid and upper(g.name) = upper('rock')
		left join albums a on t.albumid = a.albumid
		) t2
	on t1.trackid = t2.trackid
	group by t1.BillingCountry, t1.the_year, t2.album
) t3  
;
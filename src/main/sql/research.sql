-- イノベーションワードの検索
select vocab_id, proto, pos, pos1, count(*) from (
select rw.vocab_id, rw.comp_code, v.proto, v.pos, v.pos1 from report_word rw inner join vocab v on rw.vocab_id = v.vocab_id where v.innovation group by rw.vocab_id, rw.comp_code) tmp
group by vocab_id having count(*) > 3;

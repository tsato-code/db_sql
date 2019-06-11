# ------------------
# モジュールインポート
# ------------------
import sqlite3
import pandas as pd


# ------------------
# 定数パラメータ設定
# ------------------
INPUT_FILE = "ghibli.csv"


# ------------------
#  自作関数
# ------------------
def sample1():
	# ファイルがあるときはファイルを開き、ファイルがないときは DB を作成
	con = sqlite3.connect("data.db")
	# DB を閉じる
	con.close()


def sample2():
	# 自動コミットモードでファイルを開く
	con = sqlite3.connect("data.db", isolation_level=None)
	# テーブルがない場合は作成
	sql = u"""
	create table if not exists 社員 (
	  名前 varchar(10),
	  年齢 integer,
	  部署 varchar(200)
	);
	"""
	con.execute(sql)
	# レコードを登録
	sql = u"insert into 社員 values ('ほげ', 30, '企画部')"
	con.execute(sql)
	# プレースホルダとバインド値を使って登録
	sql = u"insert into 社員 values (?, ?, ?)"
	con.execute(sql, (u"ふが", 35, u"経理部"))
	con.execute(sql, (u"ぴよ", 40, u"経理部"))
	# 一括登録
	con.executemany(sql, [(u"hoge", 19, u"開発部"), (u"fuga", 50, u"経理部")])
	# レコードを削除
	con.execute(u"delete from 社員 where 名前='ほげ'")
	# レコードを更新
	con.execute(u"update 社員 set 部署='開発部部' where 名前='ふが'")
	# 抽出
	cur = con.cursor()
	cur.execute(u"select * from 社員")
	for row in cur:
	    print(row[0], row[1], row[2])
	# 保存
	con.commit()
	# ロールバック
	con.rollback()
	# DB を閉じる
	con.close()


def sample3():
	# CSV ファイルを DataFrame で読み込み
	df = pd.read_csv(INPUT_FILE, encoding="utf-8")
	# sqlite3 を開く
	con = sqlite3.connect("data.db")
	# DB に新しいテーブルを追加して DataFrame を追加
	df.to_sql("ジブリ映画", con, if_exists="replace", index=True)
	# DataFrame を消去
	del df
	# コミット
	con.commit()
	# 抽出
	cur = con.cursor()
	cur.execute("select * from ジブリ映画")
	for row in cur:
		print(row[0], row[1], row[2], row[3], row[4])
	# 抽出して DataFrame に変換
	df = pd.read_sql_query("select * from ジブリ映画", con)
	print(df.head())
	con.close()


def sample4():
	# https://www.geeksforgeeks.org/joining-three-tables-sql/
	con = sqlite3.connect("data.db", isolation_level=None)
	cur = con.cursor()
	con.execute("create table if not exists student(s_id int primary key, s_name varchar(20))")
	sql = "insert into student values (?, ?)"
	sql_bind = [
		(1, 'Jack'),
		(2, 'Rithvik'),
		(3, 'Jaspreet'),
		(4, 'Praveen'),
		(5, 'Bisa'),
		(6, 'Suraj')
	]
	con.executemany(sql, sql_bind)
	con.execute("create table if not exists marks(school_id int primary key, s_id int, score int, status varchar(20))")
	sql = "insert into marks values (?, ?, ?, ?)"
	sql_bind = [
		(1004, 1,  23,  'fail'),
		(1008, 6,  95,  'pass'),
		(1012, 2,  97,  'pass'),
		(1016, 7,  67,  'pass'),
		(1020, 3,  100, 'pass'),
		(1025, 8,  73,  'pass'),
		(1030, 4,  88,  'pass'),
		(1035, 9,  13,  'fail'),
		(1040, 5,  16,  'fail'),
		(1050, 10, 53,  'pass')
	]
	con.executemany(sql, sql_bind)
	con.execute("create table if not exists details(address_city varchar(20), email_ID varchar(20), school_id int, accomplishments varchar(50))")
	sql = "insert into details values (?, ?,  ?, ?)"
	sql_bind = [
		('Banglore',  'jsingh@geeks.com',  1020, 'ACM ICPC selected'),
		('Banglore',  'jsingh@geeks.com',  1020, 'ACM ICPC selected'),
		('Hyderabad', 'praveen@geeks.com', 1030, 'Geek of the month'),
		('Delhi',     'rithvik@geeks.com', 1012, 'IOI finalist'),
		('Chennai',   'om@geeks.com',      1111, 'Geek of the year'),
		('Banglore',  'suraj@geeks.com',   1008, 'IMO finalist'),
		('Mumbai',    'sasukeh@geeks.com', 2211, 'Made a robot'),
		('Ahmedabad', 'itachi@geeks.com',  1172, 'Code Jam finalist'),
		('Jaipur',    'kumar@geeks.com',   1972, 'KVPY finalist')
	]
	con.executemany(sql, sql_bind)
	sql = """
	select s_name, score, status, address_city, email_id, accomplishments from student s inner join marks m on
		s.s_id = m.s_id inner join details d on d.school_id = m.school_id;
	"""
	df1 = pd.read_sql_query(sql, con)
	print(df1.head())
	sql = """
	select s_name, score, status, address_city, email_id, accomplishments from
		student s, marks m, details d where s.s_id = m.s_id and m.school_id = d.school_id;
	"""
	df2 = pd.read_sql_query(sql, con)
	print(df2.head())
	con.close()


# ------------------
#  メイン処理
# ------------------
def main():
	sample1()
	sample2()
	sample3()
	sample4()


if __name__=="__main__":
	main()
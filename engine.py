import csv 
import sqlparse
import statistics
import sys

sql_query = str(sys.argv[1])
# print(sql_query)

def read_csv(filename):
	rows_1 = []

	with open(filename, 'r') as csvfile: 
		csvreader = csv.reader(csvfile) 

		for row in csvreader: 
			rows_1.append(row) 
	  
	return rows_1


def parse_meta(filename):
	meta_dict = {}
	file = open(filename)
	file_contents = file.readlines()

	for line in file_contents:
		if line == '<begin_table>\n':
			temp_working_list = []
		elif line =='<end_table>\n' or line == '<end_table>':
			meta_dict.update({temp_working_list[0]:temp_working_list[1:]})
		else:
			w = ""
			for i in line:
				if i != '\n':
					w+=i
			temp_working_list.append(w)

	for i in meta_dict:
		temp = []
		for j in meta_dict[i]:
			temp.append(i+'.'+j)
		meta_dict[i] = temp

	return meta_dict

def build_columns_rows(tableName, meta_dict):
	attributes = meta_dict[tableName]
	rows = read_csv("files/"+tableName+".csv")
	cols = {}
	
	for attr in attributes:
		cols[attr] = []

	for row in rows:
		for i in range(len(attributes)):
			cols[attributes[i]].append(row[i])

	return (cols, rows)

def build_columns(attr, rows):
	cols = {}
	for i in attr:
		cols[i] = []

	for row in rows:
		for i in range(len(attr)):
			cols[attr[i]].append(row[i])

	return cols


def JoinTables(table2, table1=None):
	if table1 == None:
		return table2
	joined_table = Table()
	
	for i in table1.attr:
		joined_table.attr.append(i)
	for i in table2.attr:
		joined_table.attr.append(i)

	for row in table1.rows:
		for row2 in table2.rows:
			temp = []
			for i in row:
				temp.append(i)
			for i in row2:
				temp.append(i)
			joined_table.rows.append(temp)
	joined_table.cols = build_columns(joined_table.attr, joined_table.rows)
	joined_table.n = len(joined_table.rows)
	return joined_table

def print_table(attr, rows):
	final_print = ""
	for i in attr:
		final_print += i + ','
	final_print = final_print[:-1]
	final_print += "\n"
	# print(final_print)
	# for i in range(6*len(attr)):
		# final_print += "_ "
	# final_print += "\n"
	# print(rows)
	for row in rows:
		for i in row:
			final_print += str(i) + ','
		final_print = final_print[:-1]
		final_print += "\n"
	# for i in range(6*len(attr)):
		# final_print += "_ "
	# final_print += "\n"
	print(final_print)



def print_tables(list_of_tables):
	if len(list_of_tables) == 1:
		t1 = list_of_tables[0]
		print_table(t1.attr, t1.rows)
	else:
		jT = None
		for i in list_of_tables:
			jT = JoinTables(i, jT)
		print_table(jT.attr, jT.rows)

def join_m_tables(list_of_tables):
	if len(list_of_tables) == 1:
		t1 = list_of_tables[0]
		return t1
	else:
		jT = None
		for i in list_of_tables:
			jT = JoinTables(i, jT)
		return jT


class Table():

	def __init__(self, tableName=None, meta_dict=None):
		if tableName != None:
			self.name = tableName
			self.attr = meta_dict[tableName]
			self.cols, self.rows = build_columns_rows(tableName, meta_dict)
			self.n = len(self.rows)
		else:
			self.name = ''
			self.attr = []
			self.cols = []
			self.rows = []
			self.n = 0

	def update(self, dict):
		self.cols = dict
		for i in dict:
			self.attr.append(i)

		for j in range(len(self.cols[self.attr[0]])):
			row = []
			for i in range(len(self.attr)):
				row.append(self.cols[self.attr[i]][j])
			self.rows.append(row)
	def table_print(self, distinct_found):
		str = ""
		temp_arr = []
		for i in self.attr:
			str += i + ','
		# str -= ','
		str = str[:-1]
		str += '\n'
		for i in self.rows:
			temp_str = ""
			for j in i:
				str += j + ','
				temp_str += j + ','

			# str -= ','
			str = str[:-1]
			temp_str = temp_str[:-1]
			temp_arr.append(temp_str)
			str += '\n'
		if not distinct_found:
			print(str)
		else:
			unique_list = []
			for i in temp_arr:
				if i not in unique_list:
					unique_list.append(i)

			str = ""
			for i in self.attr:
				str += i + ','
				# str -= ','
			str = str[:-1]
			str += "\n"
			for i in unique_list:
				str += i + "\n"
			print(str)

	def print_row_single_op(self, temp_str, num_type, col_table, ret_type, distinct_found = None):
		a = temp_str[0]
		# print("reached 2 here")
		b = temp_str[1]
		# print(distinct_found)

		found_col_in_attr = False
		main_attr = ""
		for i in self.attr:
			if a == i.split('.')[1]:
				found_col_in_attr = True
				main_attr = i
		# print(self.attr)

		if a in self.attr or found_col_in_attr:
			if b in self.attr or b in [i.split('.')[1] for i in self.attr]:
				if b in self.attr:
					working_col = b
					idx_b = self.attr.index(b)

				else:
					# print("reached idx b")
					idx_b = [i.split('.')[1] for i in self.attr].index(b)

				if a in self.attr:
					idx_a = self.attr.index(a)
				if found_col_in_attr:
					idx_a = self.attr.index(main_attr)
				index_working = []
				using_equal = False
				for i in range(len(self.rows)):
					if num_type == 0:
						if int(self.rows[i][idx_a]) >= int(self.rows[i][idx_b]):
							index_working.append(i)
					if num_type == 1:
						if int(self.rows[i][idx_a]) <= int(self.rows[i][idx_b]):
							index_working.append(i)
					if num_type == 2:
						if int(self.rows[i][idx_a]) > int(self.rows[i][idx_b]):
							index_working.append(i)
					if num_type == 3:
						if int(self.rows[i][idx_a]) < int(self.rows[i][idx_b]):
							index_working.append(i)
					if num_type == 4:
						using_equal = True
						# print("reached equals to")
						if int(self.rows[i][idx_a]) == int(self.rows[i][idx_b]):
							index_working.append(i)
				if ret_type == 1:
					return index_working
				str_print = ""
				if col_table[0] == '*':
					if using_equal:
						# print("The gods are happy")
						temp_str
						a = temp_str[0]
						# print("reached 2 here")
						b = temp_str[1]
						# print(a,b)
						str_print = ''
						for i in self.attr:
							if not i == b:
								str_print += i + ','
						str_print = str_print[0:-1]
						str_print += '\n'
						workin_idx = self.attr.index(b)

						for i in index_working:
							for j in range(len(self.rows[i])):
								if j == workin_idx:
									pass
								else:
									str_print += self.rows[i][j] + ','
							str_print = str_print[0:-1]
							str_print += '\n'




					else:
						for i in self.attr:
							str_print += i + ','
						str_print = str_print[0:-1]
						str_print += '\n'

						distinct_dict = []
						for j in index_working:
							if distinct_found != None:
								for k in self.rows[j]:
									str_print += k + ','
								str_print = str_print[0:-1]
								str_print += '\n'
							else:
								if self.rows[j] in distinct_dict:
									pass
								else:
									distinct.append(self.rows[j])
									for k in self.rows[j]:
										str_print += k + ','
									str_print = str_print[0:-1]
									str_print += '\n'
							# str_print[-1] = '\n'
						str_print = str_print[0:-1]
						str_print += '\n'
						print(str_print)
						exit()

				elif is_func(col_table[0]):
					working_attrs = []
					working_funcs = []
					for i in col_table:
						func_type = i.split('(')[0]
						col_name = i.split('(')[1].split(')')[0]
						
						if col_name in self.attr:
							working_attrs.append(col_name)
							working_funcs.append(func_type)

						else:
							found_col_name = False
							for j in self.attr:
								if j.split('.')[1] == col_name:
									if not found_col_name:
										found_col_name = True
										working_attrs.append(j)
										working_funcs.append(func_type)
									else:
										print("Specify column")
										exit()
							if found_col_name == False:
								print("No such column")
								exit()

					str_print = ""
					for i in working_attrs:
						str_print += i + ','
					str_print = str_print[0:-1]
					str_print += '\n'
					# print(working_funcs, working_attrs, index_working)
					for i in range(len(working_funcs)):
						
						working_list = []
						for j in index_working:
							working_list.append(int(self.cols[working_attrs[i]][j]))
							# print(j)
						if len(working_list) > 0:
							if working_funcs[i] == 'sum':
								str_print += str(sum(working_list)) + ','
							if working_funcs[i] == 'min':
								str_print += str(min(working_list)) + ','
							if working_funcs[i] == 'max':
								# print(working_list)
								str_print += str(max(working_list)) + ','
							if working_funcs[i] == 'average':
								str_print += str(statistics.mean(working_list)) + ','

				else:
					#col_tables are columns:
					str_print = ""
					working_attrs = []
					# print("Reached here yay")
					for i in col_table:
						# print(self.attr)
						# print([j.split('.')[1] for j in self.attr])
						# print(i)
						if i in self.attr:
							str_print += i + ','
							working_attrs.append(i)
						elif i in [j.split('.')[1] for j in self.attr]:
							for j in self.attr:
								if i == j.split('.')[1]:
									str_print += j +','
									working_attrs.append(j)
									break

					str_print = str_print[0:-1]
					str_print += '\n'

					for k in index_working:
						for l in working_attrs:
							str_print += self.cols[l][k] + ','
						str_print = str_print[0:-1]
						str_print += '\n'


					

					# print(working_attrs)
					# print(index_working)
				print(str_print)


				





					
				#if second column is another column
			else:
				# print("reached 2here2")
				#if second column is int
				idx = 0
				index_working = []
				if a in self.attr:
					idx = self.attr.index(a)
				if found_col_in_attr:
					idx = self.attr.index(main_attr)

				int_work = int(b)
				# print()
				for i in range(len(self.rows)):
					if num_type == 0:

						if int(self.rows[i][idx]) >= int_work:
							index_working.append(i)
					if num_type == 1:
						if int(self.rows[i][idx]) <= int_work:
							index_working.append(i)
					if num_type == 2:
						if int(self.rows[i][idx]) > int_work:
							index_working.append(i)
					if num_type == 3:
						if int(self.rows[i][idx]) < int_work:
							index_working.append(i)
					if num_type == 4:
						# print("reached equals to")
						if int(self.rows[i][idx]) == int_work:
							index_working.append(i)
					
				if ret_type == 1:
					return index_working
				if col_table[0] == '*':
					str_print = ""
					for i in self.attr:
						str_print += i + ','
					str_print = str_print[0:-1]
					str_print += '\n'
					for i in index_working:
						for j in self.rows[i]:
							str_print += j + ','
						str_print = str_print[0:-1]
						str_print += '\n'

				elif is_func(col_table[0]):
					working_attrs = []
					working_funcs = []
					for i in col_table:
						func_type = i.split('(')[0]
						col_name = i.split('(')[1].split(')')[0]
						
						if col_name in self.attr:
							working_attrs.append(col_name)
							working_funcs.append(func_type)

						else:
							found_col_name = False
							for j in self.attr:
								if j.split('.')[1] == col_name:
									if not found_col_name:
										found_col_name = True
										working_attrs.append(j)
										working_funcs.append(func_type)
									else:
										print("Specify column")
										exit()
							if found_col_name == False:
								print("No such column")
								exit()

					str_print = ""
					for i in working_attrs:
						str_print += i + ','
					str_print = str_print[0:-1]
					str_print += '\n'
					# print(working_funcs, working_attrs, index_working)
					for i in range(len(working_funcs)):
						
						working_list = []
						for j in index_working:
							working_list.append(int(self.cols[working_attrs[i]][j]))
							# print(j)
						if len(working_list) > 0:
							if working_funcs[i] == 'sum':
								str_print += str(sum(working_list)) + ','
							if working_funcs[i] == 'min':
								str_print += str(min(working_list)) + ','
							if working_funcs[i] == 'max':
								# print(working_list)
								str_print += str(max(working_list)) + ','
							if working_funcs[i] == 'average':
								str_print += str(statistics.mean(working_list)) + ','

				else:
					#col_tables are columns:
					str_print = ""
					working_attrs = []
					# print("")
					for i in col_table:
						# print(self.attr)
						# print([j.split('.')[1] for j in self.attr])
						# print(i)
						if i in self.attr:
							str_print += i + ','
							working_attrs.append(i)
						elif i in [j.split('.')[1] for j in self.attr]:
							for j in self.attr:
								if i == j.split('.')[1]:
									str_print += j +','
									working_attrs.append(j)
									break

					str_print = str_print[0:-1]
					str_print += '\n'

					if distinct_found:
						distinct_dict = []
						for k in index_working:
							str_temp = ''
							for l in working_attrs:
								str_temp += self.cols[l][k] + ','
							if not str_temp in distinct_dict:
								distinct_dict.append(str_temp)
								str_print += str_temp[0:-1]
								str_print += '\n'
							else:
								pass
					else:
									
						for k in index_working:
							for l in working_attrs:
								str_print += self.cols[l][k] + ','
							str_print = str_print[0:-1]
							str_print += '\n'


					

					# print(working_attrs)
					# print(index_working)
				print(str_print)

		else:
			print("column 2doesn't exist doesn't exist in table")
			exit()

			

		




metadata = "files/metadata.txt"
meta_dict = parse_meta(metadata)
table_names = []
for i in meta_dict:
	table_names.append(i)

table_list = [Table(i, meta_dict) for i in table_names]

cmd = "Select A , table2.B from table1,table2 where table1.B = table2.B AND table1.A > 10" 
cmd = sql_query
parsed = sqlparse.parse(cmd)
stmt = parsed[0]
toks = stmt.tokens

token_list = sqlparse.sql.IdentifierList(toks).get_identifiers()
str_tokens = []

for token in token_list:
	if token.is_keyword:
		str_tokens.append(token.value.lower())
	else:
		str_tokens.append(token.value)


# 		for i in toks:
# ...     if not i.is_whitespace:
# ...             print(i.value, type(i).__name__)




select_found = False
from_found = False
where_found = False
distinct_found = False

col_identifiers = []
table_identifiers = []
cond_identifiers = []

if len(str_tokens) <= 3:
	print("query format wrong. Please enter of format \"Select <column> from <table> where <condition\"")
	exit()

for i in str_tokens:

	if i == 'select':
		if select_found:
			print("query format wrong. Please enter of format \"Select <column> from <table> where <condition\"")
			break
		else:
			select_found = True
	elif i == 'from':
		if not from_found:
			if select_found:
				if where_found:
					print("query format wrong. Please enter of format \"Select <column> from <table> where <condition\"")
					break
				else:
					from_found = True
			else:
				print("query format wrong. Please enter of format \"Select <column> from <table> where <condition\"")
				break
			if where_found:
				print("query format wrong. Please enter of format \"Select <column> from <table> where <condition\"")
				break

		else:
			print("query format wrong. Please enter of format \"Select <column> from <table> where <condition\"")
			break

	elif 'where' in i.lower().split():
		if not where_found:
			if select_found:
				if from_found:
					where_found = True
				else:
					print("query format wrong. Please enter of format \"Select <column> from <table> where <condition\"")
					break
			else:
				print("query format wrong. Please enter of format \"Select <column> from <table> where <condition\"")
				break


		else:
			print("query format wrong. Please enter of format \"Select <column> from <table> where <condition\"")
			break

	elif i == 'distinct':
		if not where_found:
			if not from_found:
				if select_found:
					distinct_found = True
				else:
					print("Query format wrong")
					break
			else:
				print("Query format wrong")
				break
		else:
			print("Query format wrong")
			break



	else:
		if select_found and not from_found:
			col_identifiers.append(i)
		if from_found and not where_found:
			table_identifiers.append(i)
		if where_found:
			cond_identifiers.append(i)

	# print(i)
	# print("select_found : ",select_found)
	# print("from_found : ", from_found)
	# print("distinct_found : ", distinct_found)
	# print("where_found : ", where_found)
# col_string = col_identifiers[0]
col_table = []
tab_table = []
where_table = []

for i in col_identifiers[0].split(','):
	col_table.append(i.strip())


for i in table_identifiers[0].split(','):
	tab_table.append(i.strip())

if where_found:
	where_table = str_tokens[-1].split()

	if len(where_table) < 2:
		print("Please add a condition when using where clause")
		exit()
	else:
		cond_str = str_tokens[-1].split(where_table[0])[1]



def print_table_final(table, col_table, indexs_append, distinct_found):
	if col_table[0] == '*':
		print_str = ''
		
		for i in table.attr:
			print_str += i + ','
		print_str = print_str[0:-1]
		print_str += '\n'

		for i in indexs_append:
			for j in table.rows[i]:
				print_str += j + ','
			print_str = print_str[0:-1]
			print_str += '\n'
		print(print_str)

	elif is_func(col_table[0]):
		
		working_attrs = []
		working_funcs = []
		for i in col_table:
			func_type = i.split('(')[0]
			col_name = i.split('(')[1].split(')')[0]
			
			if col_name in table.attr:
				working_attrs.append(col_name)
				working_funcs.append(func_type)

			else:
				found_col_name = False
				for j in table.attr:
					if j.split('.')[1] == col_name:
						if not found_col_name:
							found_col_name = True
							working_attrs.append(j)
							working_funcs.append(func_type)
						else:
							print("Specify column")
							exit()
				if found_col_name == False:
					print("No such column")
					exit()

		str_print = ""
		for i in working_attrs:
			str_print += i + ','
		str_print = str_print[0:-1]
		str_print += '\n'
		# print(working_funcs, working_attrs, index_working)
		for i in range(len(working_funcs)):
			
			working_list = []
			for j in indexs_append:
				working_list.append(int(table.cols[working_attrs[i]][j]))
				# print(j)
			if len(working_list) > 0:
				if working_funcs[i] == 'sum':
					str_print += str(sum(working_list)) + ','
				if working_funcs[i] == 'min':
					str_print += str(min(working_list)) + ','
				if working_funcs[i] == 'max':
					# print(working_list)
					str_print += str(max(working_list)) + ','
				if working_funcs[i] == 'average':
					str_print += str(statistics.mean(working_list)) + ','
		print(str_print)

	else:
		str_print = ""
		working_attrs = []
		
		for i in col_table:
			# print(self.attr)
			# print([j.split('.')[1] for j in self.attr])
			# print(i)
			if i in table.attr:
				str_print += i + ','
				working_attrs.append(i)
			elif i in [j.split('.')[1] for j in table.attr]:
				for j in table.attr:
					if i == j.split('.')[1]:
						str_print += j +','
						working_attrs.append(j)
						break

		str_print = str_print[0:-1]
		str_print += '\n'

		if distinct_found:
			distinct_dict = []
			for k in indexs_append:
				str_temp = ''
				for l in working_attrs:
					str_temp += table.cols[l][k] + ','
				if not str_temp in distinct_dict:
					distinct_dict.append(str_temp)
					str_print += str_temp[0:-1]
					str_print += '\n'
				else:
					pass
		else:
						
			for k in indexs_append:
				for l in working_attrs:
					str_print += table.cols[l][k] + ','
				str_print = str_print[0:-1]
				str_print += '\n'

		# print(working_attrs)
		# print(index_working)
		print(str_print)






		


def process_where(cond_str, working_tables, col_table, num_type=None, distinct_found=None,):
	# print("Checkpoint2")
	# print(len(working_tables))
	# for i in working_tables:
	 	# print(i.attr)
	# print("Printing num_type for process_where", num_type)
	# print("Printing the condition for where", cond_str)

	table = join_m_tables(working_tables)
	# print(table.attr)
	and_found = False
	or_found = False
	if 'and' in cond_str.lower().split():
		and_found = True
	if 'or' in cond_str.lower().split():
		or_found = True
	if and_found and or_found:
		print("Please don't use AND and OR together")
		exit()

	

	if not or_found and not and_found:
		# #exit()
		if len(cond_str.split('>=')) > 1:
			temp_str = [i.strip() for i in cond_str.split('>=')]
			return table.print_row_single_op(temp_str, 0, col_table, num_type, distinct_found)

		elif len(cond_str.split('<=')) > 1:
			temp_str = [i.strip() for i in cond_str.split('<=')]
			return table.print_row_single_op(temp_str, 1, col_table, num_type, distinct_found)

		elif len(cond_str.split('>')) > 1:
			# print("reached here gods")
			temp_str = [i.strip() for i in cond_str.split('>')]
			# print(temp_str)
			return table.print_row_single_op(temp_str, 2, col_table, num_type, distinct_found)

		elif len(cond_str.split('<')) > 1:
			temp_str = [i.strip() for i in cond_str.split('<')]
			return table.print_row_single_op(temp_str, 3, col_table, num_type, distinct_found)

		elif len(cond_str.split('=')) > 1:
			temp_str = [i.strip() for i in cond_str.split('=')]
			# print("Reached the correct place")
			return table.print_row_single_op(temp_str, 4, col_table, num_type, distinct_found)

	else:
		if and_found:
			# print("Martin is wild")
			conds = [i.strip() for i in cond_str.split('AND')]
			# print(conds)
			indexs_append = []
			for i in conds:
				# print("Working on condition")
				# print(i)
				indexs_append.append(process_where(i,working_tables,col_table, 1, distinct_found))
			
			final_indices = []
			a = indexs_append[0]
			b = indexs_append[1]

			if len(a) < len(b):
				for i in a:
					if i in b:
						final_indices.append(i)
			else:
				for i in b:
					if i in a:
						final_indices.append(i)

			final_indices.sort()
			# print(final_indices.sort())
			print_table_final(table, col_table, final_indices, distinct_found)
			# print(len(indexs_append))
		else:
			conds = [i.strip() for i in cond_str.split('OR')]
			# 
			# print(conds)
			indexs_append = []

			for i in conds:
				# print("Working on condition")
				# print(i)
				indexs_append.append(process_where(i,working_tables,col_table, 1, distinct_found))
			a = indexs_append[0]
			b = indexs_append[1]
			final_indices = []
			for i in a:
				final_indices.append(i)
			for i in b:
				if not i in final_indices:
					final_indices.append(i)
			final_indices.sort()
			# print(final_indices)
			print_table_final(table, col_table, final_indices, distinct_found)
		# print("Reached here Aadil")
		# print(where_table)

		# working_str = cond_str.strip('>=')
		# working_str = cond_str.strip('>=')
		# working_str = cond_str.strip('>=')


		



working_tables = []
for table in tab_table:
	for i in table_list:
		if table == i.name:
			working_tables.append(i)

found_col_identifier = False
found_wildcard = False
# distinct_found = False
found_func = False
error_in_cols = False

def is_func(i):
	return len(i.split('(')) > 1


for i in col_table:
	if i == '*':
		if not found_col_identifier and not found_func:
			if not found_wildcard:
				found_wildcard = True
			else:
				print("Do not use 2 wildcards together")
				error_in_cols = True
				break
		else:
			print("Do not use * with other columns or functions")
			error_in_cols = True
			break
	elif is_func(i):
		if not found_col_identifier and not found_wildcard:
			found_func = True
		else:
			print("Do not use functions with columns and wildcard")
			error_in_cols = True
			break
	else:
		if not found_wildcard and not found_func:
			found_col_identifier = True
		else:
			print("Do not use columns with functions and wildcard")
			error_in_cols = True
			break

def print_func(i, working_tables):
	func_type = i.split('(')[0]
	col_name = i.split('(')[1].split(')')[0]
	jT = join_m_tables(working_tables)
	col_found = False
	col_error = False
	final_col = ''
	for j in working_tables:
		col_bool = col_name in j.attr
		col_ful_bool = j.name+"."+col_name in j.attr
		if col_bool or col_ful_bool:
			if not col_found:
				col_found = True
				if col_bool:
					final_col = col_name
				else:
					final_col = j.name+"."+col_name
			else:
				print("Please specify the column"
					+" as it exists in multiple tables")
				exit()
				col_error = True
	if not col_found:
		print("The column you have entered doesn't exist in the tables")
		exit()

	# print(final_col)
	# print(func_type)
	attr = []
	attr.append(final_col)
	# print(final_col)
	# print(attr)
	rows = jT.cols[attr[0]]
	# print(rows)
	cols = []
	for i in rows:
		cols.append(int(i))
	# print(cols)
	# print(max(cols))
	# print(func_type.lower())
	ans = 0
	if func_type.lower() == 'max':
		ans = max(cols)
	if func_type.lower() == 'sum':
		ans = sum(cols)
	if func_type.lower() == 'min':
		ans = min(cols)
	if func_type.lower() == 'average':
		ans = statistics.mean(cols)

	 
	# print_table(attr, rows)
	# print(ans, attr)
	return (ans, final_col)

def print_cols(i, working_tables):
	jT = join_m_tables(working_tables)
	col_found = False
	col_error = False
	final_col = ''
	for j in working_tables:
		col_bool = i in j.attr
		col_ful_bool = j.name + "." + i in j.attr
		if col_bool or col_ful_bool:
			if not col_found:
				col_found = True
				if col_bool:
					final_col = i
				else:
					final_col = j.name +"."+ i
			else:
				print("Please specify the column"
					+" as it exists in multiple tables")
				exit()
				col_error = True
	if not col_found:
		print("The column you have entered doesn't exist in the tables")
		exit()

	# print(final_col)
	# print(func_type)
	# attr = []
	# attr.append(final_col)
	# print(final_col)
	# print(attr)
	rows = jT.cols[final_col]
	# print(len(rows))
	# print(rows)

	# print(rows)
	# print_table(attr, rows)
	# print(ans, attr)
	# print(final_col)
	return (final_col, rows)





if not error_in_cols:
	if found_wildcard:
		if where_found:
			process_where(cond_str, working_tables, col_table, 0, distinct_found)
		else:
			print_tables(working_tables)
	if found_func:
		# print("Gonna work on this lol")
		ans_temp  = []
		attr_temp = []

		for i in col_table:
			ans , attr = print_func(i, working_tables)
			ans_temp.append(ans)
			attr_temp.append(attr)

		# print()
		# print(ans_temp)
		if where_found:
			process_where(cond_str, working_tables, col_table, 0, distinct_found)
		else:
			print_table(attr_temp, [ans_temp])

	if found_col_identifier:
		# print("reached here")
		# print(where_found)
		temp_dict = {}
		for i in col_table:
			col_temp, row_temp = print_cols(i, working_tables)
			temp_dict[col_temp] = row_temp
		col_tables = Table()
		col_tables.update(temp_dict)
		if where_found:
			# print("Checkpoint 1")
			process_where(cond_str, working_tables, col_table, 0, distinct_found)
		else:
			col_tables.table_print(distinct_found)
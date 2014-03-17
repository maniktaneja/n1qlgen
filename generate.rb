#!/usr/bin/env ruby
# encoding: utf-8

require 'rubygems'
require 'couchbase'
require 'json'
require 'iso8601'
require 'pp'
require 'date'

# more info about faker: http://faker.rubyforge.org
require 'faker'
require 'optparse'

# just output extra empty line on CTRL-C
trap("INT") do
  STDERR.puts
  exit
end

options = {
  :total_records => 1_000,
  :duplicate_rate => 0,
  :bucket => "eCommerce",
  :hostname => nil,
  :username => nil,
  :password => nil,
  :generatewhat => "all",
  :singlebucket => 1
}

OptionParser.new do |opts|
  opts.banner = "Usage: generate.rb [options]"
  opts.on("-h", "--hostname HOSTNAME", "Hostname to connect to (default: 127.0.0.1:8091)") do |v|
    host, port = v.split(':')
    options[:hostname] = host.empty? ? '127.0.0.1' : host
    options[:port] = port.to_i > 0 ? port.to_i : 8091
  end
  opts.on("-u", "--user USERNAME", "Username to log with (default: none)") do |v|
    options[:username] = v
  end
  opts.on("-p", "--passwd PASSWORD", "Password to log with (default: none)") do |v|
    options[:password] = v
  end
  opts.on("-b", "--bucket NAME", "Name of the bucket to connect to (default: #{options[:bucket]})") do |v|
    options[:bucket] = v
  end
  opts.on("-t", "--total-records NUM", Integer, "The total number of the records to generate (default: #{options[:total_records]})") do |v|
    options[:total_records] = v
  end
  opts.on("-d", "--generate-data customer|products|purchases|reviews", "What data to generate") do |v|
    options[:generatewhat] = v
  end
  opts.on_tail("-?", "--help", "Show this message") do
    puts opts
    exit
  end
end.parse!


document = nil
connection = Couchbase.connect(options)
generatedata = options[:generatewhat]

if generatedata == "all" or generatedata == "customer"
	if(options[:singlebucket] == 1)
                options[:bucket] = "eCommerce"
        else
                options[:bucket] = "customer"
        end

	options[:total_records].times do |n|
  		STDERR.printf("Loading customers ...%10d / %d\r", n + 1, options[:total_records])
		ta = Time.now - (30 * 24 * 60 * 60) * rand(13) 
		tla = ta + (30 * 24 * 60 * 60) * rand(13)  

		if(tla > Time.now)
			tla = Time.now
		end

  		creditcarddoc = {
			:cardType => Faker::Business.credit_card_type,
      			:cardNumber => Faker::Business.credit_card_number,
      			:cardExpiry => Faker::Business.credit_card_expiry_date
  		}

 		document = {
			:type => "customer",
			:customerId => "customer#{n}",
			:firstName => Faker::Name.first_name,
                	:lastName => Faker::Name.last_name,
                	:emailAddress => Faker::Internet.email(:firstname),
      			:dateAdded => ta.utc.iso8601.to_s,
      			:dateLastActive => tla.utc.iso8601.to_s, 
      			:postalCode => Faker::Address.zip_code,
      			:phoneNumber => Faker::PhoneNumber.phone_number, 
      			:ccInfo => creditcarddoc
  		}

  		connection.set("customer#{n}", document)
  		
		fJson = File.open("customer/customer#{n}.json","w")
		fJson.write(document.to_json)
		fJson.close
	end
end

if generatedata == "all" or generatedata == "products"
	if(options[:singlebucket] == 1)
		options[:bucket] = "eCommerce"
	else
		options[:bucket] = "product"
	end

	connection = Couchbase.connect(options)
	
	json = File.read('sample-products.json')
	data = JSON.parse(json)
	options[:total_records] = data.length 	

	STDERR.printf("\n");
	options[:total_records].times do |n|
  		STDERR.printf("Loading products ...%10d / %d\r", n + 1, options[:total_records])
		ta = Time.now - ((30 * 24 * 60 * 60) * rand(13))
		tm = Time.now		

		pcategories = data[n]['categories']
=begin		
		if rand() < 0.5 
			pcategories.push(Faker::Commerce.department)
		end
=end

=begin
		From time.now subtract (30 * 24 * 60 * 60) * Rand(0-12)
=end	
		document = {
			:type => "product",
			:productId => "product#{n}",
			:name => data[n]['name'],
			:description => data[n]['description'],
			:color => Faker::Commerce.color,
			:imageURL => data[n]['imageUrl'],
			:dateAdded => ta.utc.iso8601.to_s,
			:dateModified => tm.utc.iso8601.to_s,
			:unitPrice => data[n]['unitPrice'],
			:categories => pcategories,
			:reviewList => []
		}
		
		connection.set("product#{n}", document) 
  
		fJson = File.open("product/product#{n}.json","w")
                fJson.write(document.to_json)
                fJson.close
	end
end

if generatedata == "all" or generatedata == "reviews"
	if(options[:singlebucket] == 1)
                options[:bucket] = "eCommerce"
        else
                options[:bucket] = "review"
        end
        connection = Couchbase.connect(options)
	options[:total_records] = 10000

        STDERR.printf("\n");
        options[:total_records].times do |n|
                STDERR.printf("Loading reviews ...%10d / %d\r", n + 1, options[:total_records])
		t = Time.now - ((30 * 24 * 60 * 60) * rand(13))
		pId = "product" + rand(900).to_s

                document = {
			:type => "review",
                        :reviewId => "review#{n}",
                        :productId => pId,
			:customerId => "customer" + rand(1000).to_s,
                        :rating => rand(6),
			:reviewedAt => t.utc.iso8601.to_s,
                }

                connection.set("review#{n}", document)
  	
		fJson = File.open("reviews/review#{n}.json","w")
                fJson.write(document.to_json)
                fJson.close
=begin
	Update the product file to have the review id in the list
=end	
	
		fJson = File.open("product/" + pId.to_s + ".json", "r+")
		fout = fJson.read
		parsed = JSON.parse(fout)
		fJson.close

		revList = parsed['reviewList']
		revList.push("review#{n}")
				
		document = {
                        :type => "product",
                        :productId => parsed['productId'],
                        :name => parsed['name'],
                        :description => parsed['description'],
                        :color => parsed['color'],
                        :imageURL => parsed['imageURL'],
                        :dateAdded => parsed['dateAdded'],
                        :dateModified => parsed['dateModified'],
                        :unitPrice => parsed['unitPrice'],
                        :categories => parsed['categories'],
                        :reviewList => revList
                }

		fJson = File.open("product/" + pId.to_s + ".json", "w")
		fJson.write(document.to_json)
		fJson.close	
	end
end

if generatedata == "all" or generatedata == "purchases"
        options[:bucket] = "eCommerce"
        connection = Couchbase.connect(options)
        options[:total_records] = 50000

        STDERR.printf("\n");
        options[:total_records].times do |n|
                STDERR.printf("Loading purchases ...%10d / %d\r", n + 1, options[:total_records])
                t = Time.now

		num_of_items = rand(5) + 1
		num_max_per_item_count = 5

		arr_products_purchased = []
		arr_product_item_count = []

		num_of_items.times do
			document = { 
					:product => "product"+rand(900).to_s,
					:count => rand(5) + 1,
			}
			arr_products_purchased.push(document)
=begin
			arr_products_purchased.push("product" + rand(900).to_s)
			arr_product_item_count.push(rand(5) + 1)
=end
		end
=begin		
		print arr_products_purchased.to_s
		print arr_product_item_count.to_s
=end          
		document = {
			:type => "purchase",
                        :purchaseId => "purchase#{n}",
                        :customerId => "customer" + rand(1000).to_s,
                	:purchasedAt => t.utc.iso8601.to_s,
			:lineItems => arr_products_purchased,
		}

                connection.set("purchase#{n}", document)
        
		fJson = File.open("purchases/purchase#{n}.json","w")
                fJson.write(document.to_json)
                fJson.close

	end
end
STDERR.puts

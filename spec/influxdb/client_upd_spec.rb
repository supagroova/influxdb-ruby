require "spec_helper"
require "json"

describe InfluxDB::Client do

  before do
    @influxdb = InfluxDB::Client.new "database", {
      :host => "influxdb.test", :port => 9999, :udp_port => 4445, :udp_host => '127.0.0.1', :username => "username",
      :password => "password", :time_precision => "s", :use_udp => true }.merge(args)
  end

  let(:args) { {} }

  describe "#new" do

    describe "with udp option specified" do

      it "should be initialized with udp enabled" do
        @influxdb.should be_a InfluxDB::Client
        @influxdb.database.should == "database"
        @influxdb.hosts.should == ["influxdb.test"]
        @influxdb.port.should == 9999
        @influxdb.udp_host.should == '127.0.0.1'
        @influxdb.udp_port.should == 4445
        @influxdb.username.should == "username"
        @influxdb.password.should == "password"
        @influxdb.use_udp.should be_truthy
        @influxdb.use_ssl.should be_falsey
      end
      
      it "should be initialized with udp enabled with defaults" do
        @influxdb = InfluxDB::Client.new :use_udp => true

        @influxdb.should be_a InfluxDB::Client
        @influxdb.database.should be_nil
        @influxdb.hosts.should == ["localhost"]
        @influxdb.udp_host.should == "localhost"
        @influxdb.port.should == 8086
        @influxdb.udp_port.should == 4444
        @influxdb.username.should == "root"
        @influxdb.password.should == "root"
        @influxdb.use_udp.should be_truthy
        @influxdb.use_ssl.should be_falsey
      end
    end
    
  end

  describe "#write_point" do
    it "should POST to add points" do
      body = [{
        "name" => "seriez",
        "points" => [[87, "juan"]],
        "columns" => ["age", "name"]
      }].to_json
      @influxdb.socket.should_receive(:send).with(body, 0).once

      data = {:name => "juan", :age => 87}
      @influxdb.write_point("seriez", data).should be_nil
    end

    it "should POST multiple points" do
      body = [{
        "name" => "seriez",
        "points" => [[87, "juan"], [99, "shahid"]],
        "columns" => ["age", "name"]
      }].to_json
      @influxdb.socket.should_receive(:send).with(body, 0).once

      data = [{:name => "juan", :age => 87}, { :name => "shahid", :age => 99}]
      @influxdb.write_point("seriez", data).should be_nil
    end

    it "should POST multiple points with missing columns" do
      body = [{
        "name" => "seriez",
        "points" => [[87, "juan"], [nil, "shahid"]],
        "columns" => ["age", "name"]
      }].to_json
      @influxdb.socket.should_receive(:send).with(body, 0).once

      data = [{:name => "juan", :age => 87}, { :name => "shahid"}]
      @influxdb.write_point("seriez", data).should be_nil
    end

    it "should dump a hash point value to json" do
      prefs = [{'favorite_food' => 'lasagna'}]
      body = [{
        "name" => "users",
        "points" => [[1, prefs.to_json]],
        "columns" => ["id", "prefs"]
      }].to_json
      @influxdb.socket.should_receive(:send).with(body, 0).once

      data = {:id => 1, :prefs => prefs}
      @influxdb.write_point("users", data).should be_nil
    end

    it "should dump an array point value to json" do
      line_items = [{'id' => 1, 'product_id' => 2, 'quantity' => 1, 'price' => "100.00"}]
      body = [{
        "name" => "seriez",
        "points" => [[1, line_items.to_json]],
        "columns" => ["id", "line_items"]
      }].to_json
      @influxdb.socket.should_receive(:send).with(body, 0).once

      data = {:id => 1, :line_items => line_items}
      @influxdb.write_point("seriez", data).should be_nil
    end
    
  end

end

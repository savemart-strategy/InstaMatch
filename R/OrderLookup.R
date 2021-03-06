  #' Find equivalent transaction in TSMC POS system for every Order generated
#' through InstaCart.
#'
#' @param instac.df Can be either a dataframe exported from InstaCart containing all original
#' columns or string for a CSV file exported from InstaCart.
#' @param start.date Lower date limit of the search on 1010.
#' @param end.date Upper date limit of the search on 1010.
#' @param r1010.user.name String for 1010's user name. If NULL, input will be prompted in the Console.
#' @param r1010.password String for 1010's password. If NULL, input will be prompted in the Console.
#' @param r1010.keep.session TRUE to keep session open. FALSE to close session at the end of te function
#'
#' @return returns a dataframe containing InstaCart Order Id's and their equivalent
#' Transaction ID in our TMSC Pos System.
#' @export
#'
#' @examples
#'
#' @export
match_orders <- function(instac.df,
                        start.date=NULL,
                        end.date=NULL,
                        r1010.user.name=NULL,
                        r1010.password=NULL,
                        r1010.keep.session=TRUE){
  #check arguments
  if (!exists('instac.df')) {
    return(stop("instac.df not defined. instac.df argument must be a either InstaCart dataframe or a string path referencing a file"))
  }
  if (is.character(instac.df) & data.table::like(instac.df, "*.csv", ignore.case = T)) {
    instac.df <- tryCatch({read.csv(instac.df)},
                   error=stop("Couldn't read CSV file. Check path and file name"))
  } else {
    instac.df <- as.data.frame(instac.df)
  }

  print("Processing")

  #show progress bar
  pb <- winProgressBar(title = "progress bar", min = 0, max = 100, width = 500)
  update_pb(10, pb, "Preparing")

  #start
  ###################
  #Import InstaCart Data
  instac.df <- data.table(df) %>%
    select(Order.ID,
           Delivery.Date,
           Store.Location,
           UPC.PLU,
           Qty,
           Item,
           Is.Redelivered,
           Instacart.Online.Price,
           Instacart.Online.Revenue,
           Fulfilled.Item.Type) %>%
    mutate(deliv.date = as.Date(Delivery.Date)) %>%
    filter(!is.na(Qty) &
             Is.Redelivered=='false',
           Item!="Bag Fee") %>%
    #UPC.PLU=if_else(Item=="Bag Fee", 487, UPC.PLU)
    mutate(substituted.items=ifelse(Fulfilled.Item.Type=='Substitution Item'|
                                      Fulfilled.Item.Type=='Substitution Item - special request',1,0),
           #adjust UPC code to match 1010's standard
           upc=ifelse(nchar(as.character(UPC.PLU))>8,
                      substr(as.character(UPC.PLU),1,
                             nchar(as.character(UPC.PLU))-1),UPC.PLU)) %>%
    mutate(Qty=ifelse(Qty==0,  #Treat Orders with zeroed Qty
                      round(Instacart.Online.Revenue/Instacart.Online.Price,2),
                      Qty)) %>%
    select(-Delivery.Date, -UPC.PLU, -Instacart.Online.Price) %>% #remove helper cols
    group_by(Order.ID) %>% #add summarised columns
    mutate(insta.upcs.cnt=n(),
           insta.order.qty=sum(Qty),
           insta.net.sales=sum(Instacart.Online.Revenue),
           subst.items=sum(substituted.items)) %>%
    ungroup()

  #Update progress bar
  update_pb(20, pb, "Creating 1010 session")
#####################################################
# Import 1010 Data

  #Check credentials
  if (!exists('r1010.user.name') | !exists('r1010.password')) {

    #read user input
    r1010.user.name <- readline(prompt="Enter 1010's user name: ")
    r1010.password <- readline(prompt="Enter 1010's Password: ")
  } else {

   #Logon
   print("Logging on to 1010")
   sess <- newSession(r1010.user.name, r1010.password, kill = "pos")
  }
  #query stores on 1010
  stores.df <- openTable(sess, "savemart.stores", row.range = 'all') %>% select(store, city)

  # stores.df <- tryCatch({openTable(sess, "savemart.stores",
  #                           row.range = 'all') %>%
  #                         select(store, city)},
  #                         error=stop("Could't log on to 1010. Check credentials and try again"))
  #Update progress bar
  update_pb(20, pb, "Preparing to fetch data")

   #add city column
  instac.df <- instac.df %>%
    left_join(stores.df, by=c('Store.Location'='store')) %>%
    mutate(city.query=paste0("'",city,"'"))

  if (is.null(start.date) | is.null(end.date)) {
    start.date <- format(min(instac.df$deliv.date)-2, "%Y%m%d")
    end.date <- format(min(instac.df$deliv.date)+6, "%Y%m%d")
  }
   #Update progress bar
   update_pb(30, pb, "Fetching 1010 data. This may take several minutes")

   print("Fetching 1010 data. This may take several minutes")
   #Query transactions on 1010
   eitem.df <- openTable(sess, "savemart.eitem", cells = 10)
   query.text <- paste0('<base table="savemart.eitem" cols="date, transid, upc, qty"/>
                         <sel value="(between(date;', start.date,';', end.date,'))"/>
                         <sel value="(qty>0)"/>
                         <sel simple="1" value="(prodtyp<>\'Promo\')"/>
                         <sel value="(upc <> \'85249700848\' \'487\')"/>
                         <sel value="(fin_net_sales>0)"/>
                         <link table2="savemart.stores" col="store"
                                col2="store" cols="division, city"
                                type="exact"/>
                         <sel value="(city=', paste(unique(instac.df$city.query),
                                                    collapse = ' ') %>% as.factor(), ')"/>
                         <link table2="savemart.products" col="upc"
                                col2="upc" cols="posdesc" type="exact"/>
                         <colord cols="date,store,transid,upc,fin_net_sales,qty,account,
                                       city,posdesc,eitemupccnt,insta_acc"/>
                         <sel value="(~beginswith_ci(posdesc;\'CRV\')) &
                                      (~contains_ci(posdesc;\' fee \')) &
                                      (~contains_ci(posdesc;\' reusable bag \'))"/>
                         <willbe name="eitemupccnt"
                                 value="g_cnt(transid upc;)"
                                 label="eitem.upc.cnt"/>
                         <willbe name="eitem_qty"
                                 value="g_sum(transid upc;;qty)"
                                 label="eitem.qty"/>
                          <willbe name="eitem_net_sales"
                                 value="g_sum(transid upc;;fin_net_sales)"
                                 label="eitem_net_sales"/>
                          <willbe name="insta_acc"
                                 value="if(padright(account;4)=5391;1;
                                        if(padright(account;4)=5553;1;0))"/>')

  # run query
   eitem.df <- query(eitem.df, query.text, row.range = 'All')
   #Update progress bar
   update_pb(50, pb, "Processing")

   #convert date
   eitem.df$date <- as.Date(eitem.df$date)

  #Narrow DF width and add aggregated columns
   eitem.df <- eitem.df %>% select(date,
                                   store,
                                   transid,
                                   upc,
                                   qty,
                                   city,
                                   account,
                                   insta_acc,
                                   posdesc,
                                   net_sales=eitem_net_sales,
                                   eitemupccnt,
                                   eitem.qty=eitem_qty)

  ##########################################################
  #Prepare Instacart Data

  #clock in
  #start.time <- Sys.time()

  #Update progress bar
  update_pb(55, pb, "Processing transactions")
  print("Processing. Might take a few seconds")
  #Match UPCS by transaction
  insta_1010_match.df <- data.table(instac.df) %>%
    select(Store.Location,
           Order.ID,
           upc,
           Item,
           insta.net.sales) %>%
    #UPC lookup
    full_join(y=eitem.df %>%
                select(store,
                       upc,
                       transid,
                       insta_acc),
              by=c('Store.Location'='store', 'upc')) %>%
    filter(!is.na(transid) & !is.na(Order.ID)) %>%
    #add counts for each transaction
    group_by(Store.Location,
             Order.ID,
             transid) %>%
    #add aux columns
    summarise(upcs.matched.in.eitem=n(),
              insta_acc=mean(insta_acc),
              insta.net.sales=mean(insta.net.sales)) %>%
    #pick transactions with most UPC matches
    top_n(1,insta_acc) %>% #select transactions where tender type is Instacart
    top_n(1,upcs.matched.in.eitem) %>% #select transactions with most upc matches
    #add more transaction level details from Instacart
    plyr::join(y=instac.df %>%
                 select(Order.ID,
                        deliv.date,
                        insta.upcs.cnt,
                        insta.order.qty,
                        subst.items),
               by='Order.ID', type='left', match='first') %>%
    #add aux col
    mutate(perc.upc.match=round(upcs.matched.in.eitem/insta.upcs.cnt,2)) %>%
    as.data.frame() %>%
    #add more transaction level details from eitem
    #join acc and date from eitem for each instac order
    plyr::join(y=eitem.df %>% select(transid,
                                     eitem.date = date,
                                     city,
                                     eitem.qty,
                                     eitem.upc.cnt=eitemupccnt,
                                     net_sales),
               by='transid', type='left', match='first') %>%
    #add aux columns
    mutate(date.diff=as.Date(eitem.date)-as.Date(deliv.date),
           perc.upc.cnt.match=round(insta.upcs.cnt/eitem.upc.cnt,2),
           qty.abs.diff=round(abs(insta.order.qty-eitem.qty),2)) %>%
    add_count(Order.ID) %>%
    group_by(Order.ID) %>%
    #Filter best matches
    top_n(1,insta_acc) %>%
    top_n(-1,qty.abs.diff) %>%
    top_n(1,perc.upc.match) %>%
    top_n(1,abs(perc.upc.cnt.match)) %>%
    top_n(-1,abs(date.diff)) %>%
    #rearrenge columns
    select(Store.Location,
           city,Order.ID, transid,
           deliv.date, eitem.date,
           date.diff, insta_acc,
           insta.upcs.cnt, upcs.matched.in.eitem,
           subst.items, perc.upc.match,
           eitem.upc.cnt, perc.upc.cnt.match,
           insta.order.qty, eitem.qty,
           qty.abs.diff, insta.net.sales,
           net_sales)
  #Update progress bar
  update_pb(75, pb, "Finding matches in other locations")
  #partial enlapsed time
  #print(Sys.time() - start.time)

  ##########################################################
  #Lookup orders that were fulfilled in another Store
  print("Refining matching")
  insta_1010.refined.df <-
    instac.df %>%
    select(-Item, -Is.Redelivered) %>%
    #remove perfectly matched Orders from list
    anti_join(insta_1010_match.df %>%
                filter(insta_acc==1 & qty.abs.diff < 2),
              by="Order.ID") %>%
    #Look up on the city level
    full_join(eitem.df %>%
                filter(insta_acc==1) %>%
                select(store,
                       transid,
                       city,
                       upc,
                       qty,
                       date,
                       eitemupccnt,
                       insta_acc,
                       eitem.qty,
                       net_sales),
              by=c('city', 'upc')) %>%
    filter(!is.na(transid) & !is.na(Order.ID)) %>%
    #group back to order level
    group_by(Store.Location, Order.ID, transid,
             date, deliv.date, city) %>%
    #add aux columns
    summarise(insta_acc=mean(insta_acc),
              eitem.qty=mean(eitem.qty),
              insta.order.qty=mean(insta.order.qty),
              insta.upcs.cnt=mean(insta.upcs.cnt),
              eitem.upc.cnt=mean(eitemupccnt),
              upcs.matched.in.eitem=n(),
              insta.net.sales=mean(insta.net.sales),
              subst.items=mean(subst.items),
              net_sales=mean(net_sales)) %>%
    #add more columns for further selection
    mutate(perc.upc.match=round(upcs.matched.in.eitem/insta.upcs.cnt,2),
           perc.upc.cnt.match=round(insta.upcs.cnt/eitem.upc.cnt,2),
           date.diff = as.Date(date)-as.Date(deliv.date),
           qty.abs.diff=round(abs(sum(eitem.qty-insta.order.qty)),2)) %>%
    ungroup() %>%
    group_by(Order.ID) %>%
    #select best matches
    top_n(1, upcs.matched.in.eitem) %>%
    top_n(-1, qty.abs.diff) %>%
    #rearrange columns
    select(Store.Location, city, Order.ID, transid, deliv.date,
           eitem.date=date, date.diff, insta_acc, insta.upcs.cnt,
           upcs.matched.in.eitem, subst.items, perc.upc.match, eitem.upc.cnt,
           perc.upc.cnt.match, insta.order.qty,
           eitem.qty, qty.abs.diff, insta.net.sales, net_sales)
  #Update progress bar
  update_pb(85, pb, "Processing")
  #partial enlapsed time
  #print(Sys.time() - start.time)

  #Append Orders matched by city to orders matched by upc
  insta_1010.refined.df <- insta_1010_match.df %>%
    bind_rows(insta_1010.refined.df) %>%
    #Select best matches between lookup by city and by upc
    top_n(1,upcs.matched.in.eitem) %>%
    top_n(-1,qty.abs.diff) %>%
    top_n(1,abs(perc.upc.cnt.match)) %>%
    top_n(-1,abs(date.diff))%>%
    #remove possible duplicates in case lookup by city and by upc returns the same order
    distinct_all()

  #partial enlapsed time
  #print(Sys.time() - start.time)

  #Update progress bar
  update_pb(90, pb, "Processing")
  ##########################################################
  #Lookup orders with insta tender type having exact same net_sales total in the same store.
  insta_1010.refined.df <-
    instac.df %>%
    select(Store.Location, Order.ID,
           insta.upcs.cnt, insta.net.sales,
           insta.order.qty, deliv.date, subst.items) %>%
    #remove perfectly matched Orders
    anti_join(insta_1010.refined.df %>%
                filter(insta_acc==1 & qty.abs.diff < 1),
              by="Order.ID") %>%
    #Look up on the city level
    left_join(y=eitem.df %>%
                select(store,
                       city,
                       transid,
                       eitem.date=date,
                       net_sales,
                       insta_acc,
                       eitem.qty,
                       eitem.upc.cnt=eitemupccnt) %>%
                filter(insta_acc==1),
              by=c('Store.Location'='store',
                   'insta.net.sales'='net_sales')) %>% #Consider Including Category in the lookup
    filter(!is.na(transid)) %>%
    distinct_all() %>%
    #add aux columns
    mutate(date.diff=as.Date(eitem.date)-as.Date(deliv.date),
           perc.upc.cnt.match=round(insta.upcs.cnt/eitem.upc.cnt,2),
           qty.abs.diff=round(abs(eitem.qty-insta.order.qty),2),
           upcs.matched.in.eitem=insta.upcs.cnt,
           perc.upc.match=round(upcs.matched.in.eitem/insta.upcs.cnt,2),
           net_sales=insta.net.sales) %>%
    #rearrange columns
    select(Store.Location, city, Order.ID, transid, deliv.date,
           eitem.date, date.diff, insta_acc, insta.upcs.cnt,
           upcs.matched.in.eitem, perc.upc.match, eitem.upc.cnt,
           subst.items, perc.upc.cnt.match, perc.upc.cnt.match,
           insta.order.qty, eitem.qty, qty.abs.diff,
           insta.net.sales, net_sales) %>%
    #merge to main dataframe
    bind_rows(insta_1010.refined.df) %>%
    distinct_all() %>%
    group_by(Order.ID) %>%
    #pick best matches
    top_n(1,insta_acc) %>%
    top_n(-1,(insta.net.sales-net_sales)) %>%
    top_n(-1,qty.abs.diff) %>%
    top_n(1,abs(perc.upc.cnt.match)) %>%
    top_n(-1,abs(date.diff)) %>%
    top_n(-1,upcs.matched.in.eitem)
  #partial enlapsed time
  #print(Sys.time() - start.time)

  #Close 1010 session
  if(!r1010.keep.session) logoutSession(sess)
  print("Complete")

  #Update progress bar
  update_pb(100, pb, "Finishing")
  close(pb)

  return(insta_1010.refined.df)
}

update_pb <- function(perc_done, pb, label=NULL){
  #Update progress bar
  setWinProgressBar(pb, perc_done, title=paste(perc_done,"% done"), label = label)
}

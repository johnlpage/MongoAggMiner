

//Bitcoin Mining in MongoDB Aggregation
//Generating SHA256 Hashes
//https://en.wikipedia.org/wiki/SHA-2
//Inital Hash Values
// https://blockexplorer.com/block/0000000000000000e067a478024addfecdc93628978aa52d91fabd4292982a50
// All values HEX and correct way round
// https://en.bitcoin.it/wiki/Block_hashing_algorithm

  k = [
   0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
   0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
   0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
   0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
   0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
   0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
   0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
   0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2]

   h = [ 0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c,0x1f83d9ab, 0x5be0cd19 ]


block = {
    version: "02000000",
    previoushash: "000000000000000117c80378b8da0e33559b5997f2ad55e2f7d18ec1975b9717",
    merkelroot: "871714dcbae6c8193a2bb9b2a69fe1c0440399f38d94b3a0f1b447275a29978a",
    timestamp: "53058b35",
    bits: "19015f53",
    nonce: "33087548"
}

testsize =1000
batchsize=1000
//We can reverse the bits as we need to up front

db=db.getSiblingDB("bitcoin")
db.miner.drop()


function bytereverse(str) {
    l = str.length
    ns = ""
    for (r = 0; r < l; r = r = r + 2) {
        ns = str.substring(r, r + 2) + ns
    }
    return ns
}

function hexToIntArray(str)
{
  rval = [];
  for(b=0;b<str.length;b=b+8) {
    hexdigits = str.substring(b,b+8)
    rval.push(parseInt(hexdigits,16))
  }
  return rval
}


block.msg = block.version + bytereverse(block.previoushash) + bytereverse(block.merkelroot) + bytereverse(block.timestamp) + bytereverse(block.bits) +  bytereverse(block.nonce)
block.data = hexToIntArray(block.msg)
block.data.push(0x80000000) //Set a bit
//Pad with zeros
for(p=0;p<10;p++) block.data.push(0)
block.data.push(640) //Length of data block
printjson(block)

batch=[]
for(x=0;x<testsize;x++) {
batch.push(block)
if(batch.length >= batchsize) {
db.miner.insertMany(batch)
batch=[]
}
}

if(batch.length > 0) {
db.miner.insertMany(batch)
}
print("GO!")
pipeline = []

function getbit(integer,bit) {
    return { $let : {
                    vars : { integer: integer, bit: bit} ,
                    in : { $mod : [ shiftright("$$integer","$$bit") ,2 ] }
                     }
          }

}

function binand(integer1,integer2) {
    return { $let : {
      vars : { andint1: integer1,
                andint2: integer2},
      in :{  $reduce : {
        input : { $range : [31,-1,-1] },
        initialValue : 0 ,
        in: {  $add: [ {$multiply : [ "$$value",2 ]} ,
                         {$cond : [ {$eq : [ 2, { $add : [ getbit("$$andint1","$$this"),
                                               getbit("$$andint2","$$this") ] }]},1,0]}]
            }
        }
    }
}
}
}


function binnot(integer1) {
    return { $let : {
          vars : { notint: integer1 },
      in :{  $reduce : {
        input : { $range : [31,-1,-1] },
        initialValue : 0 ,
        in: {  $add: [ {$multiply : [ "$$value",2 ]} ,
                       {$cond : [ {$eq : [ 1,  getbit("$$notint","$$this")]},0,1]} ]}
        }
    }
  }
}
}



function binor(a,b) {
    return {    $let: {
            vars: {
                a: a,
                b: b
            },
            in :{ $reduce : {
        input : { $range : [31,-1,-1] },
        initialValue : 0 ,
        in: {  $add: [ {$multiply : [ "$$value",2 ]} ,
                         {$cond : [ {$ne : [ 0, { $add : [ getbit("$$a","$$this"),
                                               getbit("$$b","$$this") ] }]},1,0]}]}
                   }
                  }
           }
        }
}

function binxor(a,b) {
    return { $let : {
      vars:{a:a,b:b},
    in: {  $reduce : {
        input : { $range : [31,-1,-1] },
        initialValue : 0 ,
        in: {  $add: [ {$multiply : [ "$$value",2 ]} ,
                         {$cond : [ {$eq : [ 1, { $add : [ getbit("$$a","$$this"),
                                               getbit("$$b","$$this") ] }]},1,0]}]
            }
        }
    }
  }
}}

function binxor3way(a,b,c) {
    return { $let : {
      vars:{a:a,b:b,c:c},
    in: {  $reduce : {
        input : { $range : [31,-1,-1] },
        initialValue : 0 ,
        in: {  $add: [ {$multiply : [ "$$value",2 ]} ,
                         {$mod:[{$add : [ getbit("$$a","$$this"),getbit("$$b","$$this"), getbit("$$c","$$this")]},2]}]
            }
        }
    }
  }
}}

function binxor3way_new(a,b,c) {
    return { $let : {
      vars:{a:a,b:b,c:c},
    in: {  $reduce : {
        input : { $range : [31,-1,-1] },
        initialValue : 0 ,
        in: {  $add: [ {$multiply : [ "$$value",2 ]} ,
                         {$arrayElemAt : [ [ 0,1,0,1] , {$add : [ getbit("$$a","$$this"),getbit("$$b","$$this"), getbit("$$c","$$this")]} ]}]
            }
        }
    }
  }
}}




function binxor3way_new2(a,b,c) {
    return { $let : {
      vars:{a:a,b:b,c:c},
    in: {  $reduce : {
        input : { $range : [31,-1,-1] },
        initialValue : 0 ,
        in: {  $add: [ {$multiply : [ "$$value",2 ]} ,
                         {$cond :[ {$in : [{$add: [ getbit("$$a","$$this"),getbit("$$b","$$this"), getbit("$$c","$$this")]},[1,3]]},1,0]}
                      ]
            }
        }
    }
  }
}}



//Represent Binary numbers as strings(compact and fast)
function shiftleft(integer,bits) {
    return {$mod : [{$floor: {$multiply : [ integer, {$pow : [ 2, bits ]} ]}},0xffffffff]}
}

//Simple version
function shiftright_1(integer,bits) {
    return {$floor: {$divide : [ integer, {$pow : [ 2, bits ]} ]}}
}

function shiftright_null(integer,bits)
{
  return integer
}

function shiftright_nofloor(integer,bits)
{
   {$floor: {$divide : [ integer, {$arrayElemAt:[powtwo,bits]}]}}
}

var powtwo = []

for(b=0;b<32;b++) { powtwo[b]= Math.pow(2,b)}
//print(powtwo)


//Use array lookup
function shiftright(integer,bits) {
    return {$floor: {$divide : [ integer, {$arrayElemAt:[powtwo,bits]}]}}
}


//Use Switch
function shiftright_3(integer,bits) {
  branches = []
  for(b=0;b<31;b++) {
    branches.push({case:{$eq:[bits,b]},then:1<<b})
  }

  var divisor = { $switch: {
   branches: branches,
   default: 1
}}
  return {$floor: {$divide : [ integer, divisor]}}
}

function shiftright_5(integer,bits) {
    return {$trunc: {$divide : [ integer, {$arrayElemAt:[powtwo,bits]}]}}
}


//Replace floor with odd/even check
function shiftright_4(integer,bits) {
    integer = {$subtract:[integer,{$mod:[integer,{$arrayElemAt:[powtwo,bits]}]}]}
    return {$divide : [ integer, {$arrayElemAt:[powtwo,bits]}]}
}



function rightrotate(integer, bits) {
    return {
        $let: {
            vars: {
                integer: integer,
                bits: bits
            },
            in:  binor(shiftleft("$$integer", {
                        $subtract: [32, "$$bits"]
                    }),shiftright("$$integer", "$$bits"))
            
        }
    }
}



function s0(a,i) {
  
  return  {$let : {
          vars : {n:{$arrayElemAt:[a,{$subtract:[i,15]}]}},
          in : binxor3way(rightrotate("$$n",7),
             rightrotate("$$n",18),
            shiftright("$$n",3))
    }
  }
}




function s1(a,i) {
  return  {$let : {
          vars : { n:{$arrayElemAt:[a,{$subtract:[i,2]}]} },
          in : binxor3way(rightrotate("$$n",17),
             rightrotate("$$n",19),
      shiftright("$$n",10))
    }
  }
}

function floatToUnsignedInt(flt){
  return { $let : {
    vars : { ftui:flt },
    in : binand( "$$ftui", 0xffffffff)
      }
  }
}


function roundsigma1_orig(vals){
  return { $let : { 
                vars: {rs1e:vals+".e"},
                in: binxor(binxor(rightrotate("$$rs1e",6),rightrotate("$$rs1e",11)),rightrotate("$$rs1e",25))
        }
    }
}

function roundsigma1(vals){
  return { $let : { 
                vars: {rs1e:vals+".e"},
                in: binxor3way(rightrotate("$$rs1e",6),rightrotate("$$rs1e",11),rightrotate("$$rs1e",25))
        }
    }
}

function roundsigma0(vals){
  return { $let : { 
                vars: {rs2a:vals+".a"},
                in: binxor3way(rightrotate("$$rs2a",2),rightrotate("$$rs2a",13),rightrotate("$$rs2a",22))
        }
    }
}

function majority_old(vals) {
 return {
    $let : {
      vars: { maja:vals+".a",majb:vals+".b",majc:vals+".c"},
      in: binxor3way(binand("$$maja","$$majb"),binand("$$maja","$$majc"),binand("$$majb","$$majc"))
    }
  }
}



function majority(vals) {
    return { $let : {
      vars:{a:vals+".a",b:vals+".b",c:vals+".c"},
    in: {  $reduce : {
        input : { $range : [31,-1,-1] },
        initialValue : 0 ,
        in: {  $add: [ {$multiply : [ "$$value",2 ]} ,
                         {$arrayElemAt : [ [0,0,1,1],{ $add : [ getbit("$$a","$$this"),getbit("$$b","$$this"), getbit("$$c","$$this")]}]}]
            }
        }
    }
  }
}}
/*
db.test.drop()
db.test.insert({s:{a:2,b:0,c:3}})
pipe = {$addFields:{v:majority_new("$s")}}
printjson(pipe)
printjson(db.test.aggregate(pipe).next())
quit()*/


//E decides whther to take F or G
function choose_new(vals){
  return {
    $let : {
      vars: { e:vals+".e",f:vals+".f",g:vals+".g"},
      in: {  $reduce : {
        input : { $range : [31,-1,-1] },
        initialValue : 0 ,
        in: {  $add: [ {$multiply : [ "$$value",2 ]} ,
                         {$arrayElemAt : [ [getbit("$$g","$$this"), getbit("$$f","$$this")],getbit("$$e","$$this")]}]
            }
        }
      }
    }
  }
}


function choose(vals){
  return {
    $let : {
      vars: { che:vals+".e",chf:vals+".f",chg:vals+".g"},
      in: binxor(binand("$$che","$$chf"),binand(binnot("$$che"),"$$chg"))
    }
  }
}


function temp2(vals)
{
  return {$add : [ roundsigma0(vals),majority(vals)]}
}

function temp1(vals,idx)
{
  return { $add : [vals+".h",roundsigma1(vals),choose(vals),{$arrayElemAt:[k,idx]},{$arrayElemAt:["$M",idx]}]}
}


//Save Seeds
function add32bit(a,b) {
  return binand({$add:[a,b]},0xffffffff)
}


//Create a Message Array, first 16

var copyBlock1 = { $addFields : { M :{ $slice : ["$data",0,16]}}}

pipeline.push(copyBlock1)
//printjson(pipeline)


//Now copy shifted values into block

var extendMessage = {
    $addFields: {
        M: {
            $reduce: {
                input: {
                    $range: [16, 64]
                },
                initialValue: "$M",
                in: {
                    $concatArrays: ["$$value", [floatToUnsignedInt({
                            $add: [{
                                    $arrayElemAt:["$$value", {
                                        $subtract: ["$$this", 16]
                                    }]
                                },
        s0("$$value", "$$this"),
                                {
                                    $arrayElemAt:["$$value", {
                                        $subtract: ["$$this", 7]
                                    }]
                                },
        s1("$$value", "$$this")]
                        })
                    ]]
                }

            }
        }
    }
}


pipeline.push(extendMessage)

//Assign Seeds (We only do this at the start of a round)
var addInitialHash = { $addFields : {s:{ a: h[0], b: h[1], c: h[2], d: h[3], e: h[4], f:h[5], g:h[6],h:h[7]}}}
pipeline.push(addInitialHash)





var hashround = { $addFields : { new_s : {
  $reduce : {
      input : { $range : [0,64,1] },
        initialValue : "$s" ,
        in: { 
              h:"$$value.g",
              g:"$$value.f",
              f:"$$value.e",
              e:binand({ $add: [ "$$value.d", temp1("$$value","$$this")]},0xffffffff),
              d:"$$value.c",
              c:"$$value.b",
              b:"$$value.a",
              a:binand({$add:[temp1("$$value","$$this"),temp2("$$value")]},0xffffffff)

            }
  }

}}}

pipeline.push(hashround)



var saveseeds = { $addFields : {s:{ a: add32bit("$s.a","$new_s.a"), b: add32bit("$s.b","$new_s.b"),
                                    c: add32bit("$s.c","$new_s.c"),d: add32bit("$s.d","$new_s.d"),
                                    e: add32bit("$s.e","$new_s.e"), f: add32bit("$s.f","$new_s.f"),
                                    g: add32bit("$s.g","$new_s.g"), h: add32bit("$s.h","$new_s.h")}}}

pipeline.push(saveseeds)



//Now we have a second block to hash (for Bitcoin)

//Put the next chunk in the message blockj

var copyBlock2 = { $addFields : { M :{ $slice : ["$data",16,16]}}}

pipeline.push(copyBlock2)
//Now complete the message block
pipeline.push(extendMessage)


//Another hashing round

pipeline.push(hashround)

//Anf keep the result
pipeline.push(saveseeds)


//Now we need to build the Message Buffer again fro the result
var reHash = {$addFields :  { "M" : [ "$s.a","$s.b","$s.c","$s.d","$s.e","$s.f","$s.g","$s.h",2147483648,0,0,0,0,0,0,256]}}
pipeline.push(reHash)

pipeline.push(addInitialHash) //This is a new sha256 so start from the beginning

pipeline.push(extendMessage)

//Another hashing round

pipeline.push(hashround)

//Anf keep the result
pipeline.push(saveseeds)

//printjson(pipeline)
var t1 = new Date().getTime();

var cursor = db.miner.aggregate(pipeline)
var r
while(cursor.hasNext()) {
  r = cursor.next()
}
printjson(r)

var t2=new Date().getTime();


var mstaken = (t2-t1) 
if(mstaken > 0) {
var speed = (testsize * 1000) / mstaken
}
print("Speed: " + speed + " hashes/s")
print(mstaken / testsize + "ms per hash")

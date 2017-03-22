// Event handling
document.addEventListener("DOMContentLoaded",
  function (event) {
    
    // Unobtrusive event binding
    document.querySelector("button")
      .addEventListener("click", function () {

          
        
        
        $ajaxUtils
          .sendGetRequest("jso.json", 
            function (res) {
                          
                          if(document.querySelector("input").value===res.name1){
                        

                            document.getElementById("ques").innerHTML="<h2>" + res.ques1+ "</h2>";
                            document.querySelector("input").type="radio";
                             document.getElementById("i1").name="true";
                             document.getElementById("i1").name="true"; 


                             if(document.getElementById("i1").checked){
                             
                              if(document.getElementById("i1").value===res.an1){
                              
                                document.getElementById("content").innerHTML="<h2>your answer is correct<h2>"
                              }
                             
                             }else if (document.getElementById("i2").checked) {
                              if(document.getElementById("i2").value!=res.an1){
                              
                                document.getElementById("content").innerHTML="<h2>your answer is incorrect<h2>"
                              }

                             }
                           }



                           if(document.querySelector("input").value===res.name2){
                        

                            document.getElementById("ques").innerHTML="<h2>" + res.ques2 "</h2>";
                            document.querySelector("input").type="radio";
                             document.getElementById("i1").name="true";
                             document.getElementById("i1").name="true"; 


                             if(document.getElementById("i1").checked){
                             
                              if(document.getElementById("i1").value===res.ans2){
                              
                                document.getElementById("content").innerHTML="<h2>your answer is correct<h2>"
                              }
                             
                             }else if (document.getElementById("i2").checked) {
                              if(document.getElementById("i2").value!=res.ans2){
                              
                                document.getElementById("content").innerHTML="<h2>your answer is incorrect<h2>"
                              }

                             }
                           }
                           


                                                    


                            




                          
              

             
            });
      });
  }
);

$(document).ready(function(){
  urlPage = document.URL;
  //console.log('URL: ', urlPage);
  var splittedUrl = urlPage.split('/');
//  for (urlPart of splittedUrl) {
//    console.log(urlPart);
//  }
  var allServices = ''; // Contiene il json di TUTTI i servizi dell'ultima chiamata ajax effettuata
  // 'http://localhost:8084/bds_tools/Schedulatore';  IN LOCALE
//  console.log(urlPage);
//  console.log(splittedUrl[0]);
//  console.log(splittedUrl[1]);
//  console.log(splittedUrl[2]);
  var servletUrl = splittedUrl[0] + '//' + splittedUrl[2] + '/bds_tools/Schedulatore';
//  console.log(servletUrl);

  // Funzione che ritorna l'icona del servizio dando in input il nome del servizio
    function getGlyphicon(service, personalClasses){
      var glyphicon = '';
      switch (service) {
        case 'PulitoreMongoDownload':
          glyphicon = '<svg class="' + personalClasses + '" fill="#000000" height="48" viewBox="0 0 24 24" width="48" xmlns="http://www.w3.org/2000/svg">' +
                        '<path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/>' +
                        '<path d="M0 0h24v24H0z" fill="none"/>' +
                      '</svg>';
          break;
        case 'AzzeratoreUidEmailScaricate':
          glyphicon = '<svg class="' + personalClasses + '" fill="#000000" height="48" viewBox="0 0 24 24" width="48" xmlns="http://www.w3.org/2000/svg">' +
                        '<path d="M18 4V3c0-.55-.45-1-1-1H5c-.55 0-1 .45-1 1v4c0 .55.45 1 1 1h12c.55 0 1-.45 1-1V6h1v4H9v11c0 .55.45 1 1 1h2c.55 0 1-.45 1-1v-9h8V4h-3z"/>' +
                        '<path d="M0 0h24v24H0z" fill="none"/>' +
                      '</svg>';
          break;
        case 'PulitoreCestinoMongo':
          glyphicon = '<svg class="' + personalClasses + '" fill="#000000" height="48" viewBox="0 0 24 24" width="48" xmlns="http://www.w3.org/2000/svg">' +
                        '<path d="M0 0h24v24H0V0z" fill="none"/>' +
                        '<path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zm2.46-7.12l1.41-1.41L12 12.59l2.12-2.12 1.41 1.41L13.41 14l2.12 2.12-1.41 1.41L12 15.41l-2.12 2.12-1.41-1.41L10.59 14l-2.13-2.12zM15.5 4l-1-1h-5l-1 1H5v2h14V4z"/>' +
                        '<path d="M0 0h24v24H0z" fill="none"/>' +
                      '</svg>';
          break;
        case 'PubblicatoreAlbo':
          glyphicon = '<svg class="' + personalClasses + '" fill="#000000" height="48" viewBox="0 0 24 24" width="48" xmlns="http://www.w3.org/2000/svg">' +
                        '<path d="M7 19h10V4H7v15zm-5-2h4V6H2v11zM18 6v11h4V6h-4z"/>' +
                        '<path d="M0 0h24v24H0z" fill="none"/>' +
                      '</svg>';
          break;
        case 'Spedizioniere':
          glyphicon = '<svg class="' + personalClasses + '" fill="#000000" height="48" viewBox="0 0 24 24" width="48" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">' +
                        '<defs>' +
                            '<path d="M0 0h24v24H0V0z" id="a"/>' +
                        '</defs>' +
                        '<clipPath id="b">' +
                            '<use overflow="visible" xlink:href="#a"/>' +
                        '</clipPath>' +
                        '<path clip-path="url(#b)" d="M2.5 19h19v2h-19zm19.57-9.36c-.21-.8-1.04-1.28-1.84-1.06L14.92 10l-6.9-6.43-1.93.51 4.14 7.17-4.97 1.33-1.97-1.54-1.45.39 1.82 3.16.77 1.33 1.6-.43 5.31-1.42 4.35-1.16L21 11.49c.81-.23 1.28-1.05 1.07-1.85z"/>' +
                        '<path clip-path="url(#b)" d="M0 0h24v24H0V0z" fill="none"/>' +
                      '</svg>';
          break;
        case 'CreatoreFascicoloSpeciale':
          glyphicon = '<svg class="' + personalClasses + '" fill="#000000" height="48" viewBox="0 0 24 24" width="48" xmlns="http://www.w3.org/2000/svg">' +
                        '<path d="M0 0h24v24H0V0z" fill="none"/>' +
                        '<path d="M20 6h-8l-2-2H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm-2.06 11L15 15.28 12.06 17l.78-3.33-2.59-2.24 3.41-.29L15 8l1.34 3.14 3.41.29-2.59 2.24.78 3.33z"/>' +
                      '</svg>';
          break;
        case 'VersatoreParer':
          glyphicon = '<svg class="' + personalClasses + '" fill="#000000" height="48" viewBox="0 0 24 24" width="48" xmlns="http://www.w3.org/2000/svg">' +
                        '<path d="M16 17.01V10h-2v7.01h-3L15 21l4-3.99h-3zM9 3L5 6.99h3V14h2V6.99h3L9 3z"/>' +
                        '<path d="M0 0h24v24H0z" fill="none"/>' +
                      '</svg>';
          break;
        default:

      }
      var tmpStyle= '';
      if (personalClasses) {
        tmpStyle = personalClasses;
      }
      return glyphicon;
    }

    // Funzione per messaggi temporanei
    function showTemporaryMessage(message){
      var formattedMessage = '<div id="temporaryMessage" class="alert alert-warning alert-dismissible col-lg-2 col-md-3 col-sm-6 col-xs-12 temporaryMessage" role="alert">' +
                                '<button type="button" class="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">&times;</span></button>' +
                                message +
                              '</div>';
      $('#bodyId').append(formattedMessage);
      setTimeout(function(){hideTemporaryMessage()}, 5000);
    }

    // Funzione nasconde il messaggio temporaneo
    function hideTemporaryMessage(){
      var messageWrapper = document.getElementById("temporaryMessage");
      messageWrapper.parentNode.removeChild(messageWrapper);
    }

    function showAllServices(oldJson, counter) {
      $.ajax({
        method: 'POST',
        url: servletUrl,
        data: {schedulatore: 'json'},
        success: function(data){
          if (data) {
            if (data.jobs) {
              if (oldJson) {
                if (oldJson === data) {
                  if (counter <= 3) {
                      counter++;
                    setTimeout(function(){showAllServices(data, counter)}, 1000);
                  }else {
                    showTemporaryMessage('Deve essersi verificato un errore lato server:<br/>Il Json ritornato dal server è uguale a quello attuale.');
                  }
                }else {
                  attachToTheWall(data);
                }
              }else {
                attachToTheWall(data);
              }

              // $('#PulitoreMongoDownload').click(function(){showDetailService('PulitoreMongoDownload')});
              // $('#AzzeratoreUidEmailScaricate').click(function(){showDetailService('AzzeratoreUidEmailScaricate')});
              // $('#PulitoreCestinoMongo').click(function(){showDetailService('PulitoreCestinoMongo')});
              // $('#PubblicatoreAlbo').click(function(){showDetailService('PubblicatoreAlbo')});
              // $('#Spedizioniere').click(function(){showDetailService('Spedizioniere')});
              // $('#CreatoreFascicoloSpeciale').click(function(){showDetailService('CreatoreFascicoloSpeciale')});
              // $('#VersatoreParer').click(function(){showDetailService('VersatoreParer')});
            }
          }
        },
        error: function(jqXHR, textStatus, errorThrown){
//            console.log('Qui ' + counter)
          if (!counter) {
            counter = 1;
          }
          if (counter <= 3) {
              counter++;
            setTimeout(function(){showAllServices(oldJson, counter)}, 1000);
          }else {
            console.error(jqXHR, textStatus, errorThrown);
            showTemporaryMessage('Si è verificato un errore nella chiamata al server.<br>Guardare la console dei log per maggiori informazioni.');
          }
        }
      });
    }

    function attachToTheWall(data){
      allServices = data;
      $('#mainContent').text(''); // Svuoto il div prima di ricaricarlo
      for (var i = 0; i < data.jobs.length; i++) {
          $('#mainContent').append(formatService(data.jobs[i]));
          addOnclickToShowDetaiService(data.jobs[i].name);
          addOnclickChangeServiceStatus(i);
      }
    }

    function addOnclickToShowDetaiService(serviceName){
      $('#' + serviceName).click(function(){showDetailService(serviceName)});
    }

    function addOnclickChangeServiceStatus(serviceIndex){
      $('#switcher_' + allServices.jobs[serviceIndex].name).click(function(){changeServiceStatus(serviceIndex)});
    }

    function showActiveServices(status) {
      status =  status === true ? 'true' : 'false'; // Questo perchè il campo active del json non è un booleano ma una stringa. Mi servirà dopo per il confronto.
      $.ajax({
        method: 'POST',
        url: servletUrl,
        data: {schedulatore: 'json'},
        success: function(data){
          if (data) {
            if (data.jobs) {
              allServices = data;
              $('#mainContent').text(''); // Svuoto il div prima di ricaricarlo
              var atLeastOne = false;
              for (var i = 0; i < data.jobs.length; i++) {
                if (data.jobs[i].active === status) { // Active è stringa status è stringa
                  atLeastOne = true;
                  $('#mainContent').append(formatService(data.jobs[i]));
                }
              }
              if (!atLeastOne) {
                status = status === true ? 'attivo' : 'stoppato';
                var message = 'Nessun servizio è ' + status + ' al momento';
                //var message = 'fsdsfgdsfgfdgdfgad ' + 'gsdgfsgfgadfadfgdfgafgf' + ' gfsgfdgdafgdfg' + ' ergdfagetgae';
                showTemporaryMessage(message);
              }
            }
          }
        },
        error: function(jqXHR, textStatus, errorThrown){
          console.error(jqXHR, textStatus, errorThrown);
        }
      });
    }

    function formatService(service){
      var ringColor = service.active === 'true' ? 'green' : 'red';
      var switcher = service.active === 'true' ? 'deactivate' : 'activate';
      var htmlSwitcher =  '<div id="switcher_' + service.name + '" class="singleBoxBottom">' +
                            '<span class="displayBlock padding15">' + switcher + '</span>' +
                          '</div>';
      var htmSvg = '<div class="singleBoxTop">' +
                    '<svg fill="' + ringColor + '" height="24" viewBox="0 0 24 24" width="24" xmlns="http://www.w3.org/2000/svg">' +
                      '<path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-3.58 8-8 8z"/>' +
                      '<path d="M0 0h24v24H0z" fill="none"/>' +
                    '</svg>' +
                  '</div>';
      var htmlName = '<div class="singleBoxBody">' +
                        '<div class="singleBoxInnerBody">' +
                          getGlyphicon(service.name, 'heightNWidth90 fillOpacBlue') +
                          '<span class="displayBlock paddingTop15 serviceNameStyle">' + service.name + '</span>' +
                        '</div>' +
                      '</div>';
      var box = '<div class="col-lg-3 col-md-4 col-sm-6 col-xs-12 padding15">' +
                  '<a href="#">' +
                    '<div class="fillParent singleBox borderRadius boxShadow">' +
                      '<div id="' + service.name + '" class="displayTable fillParent padding15">' +
                        htmSvg +
                        htmlName +
                      '</div>' +
                      htmlSwitcher +
                    '</div>' +
                  '</a>' +
                '</div>';
      return box;
    }

    function showDetailService(serviceName){
      for (service of allServices.jobs) {
        if (service.name === serviceName) {
          $('#mainContent').text(''); // Svuoto il div prima di ricaricarlo
          $('#mainContent').append(formatDetailService(service));
          attacheFunctionsToButtons(service);
          break;
        }
      }
    }

    function formatDetailService(service){
      var htmlTop = '<div class="fillWidth">' +
                    '<div class="textAlignCenter fillWidth padding30">' +
                      getGlyphicon(service.name, 'heightNWidth90 fillOpacBlue') +
                    '</div>' +
                    '<div class="textAlignCenter lineHeight84 borderTopNBottom">' +
                      '<span class="serviceNameStyle">' +
                        service.name +
                      '</span>' +
                    '</div>' +
                  '</div>';
      var tableWrapper = getFormattedParams(service);
      if (!tableWrapper) {
        tableWrapper = '<div class="fillWidth textAlignCenter">This service has no params</div>';
      }
      var serviceSchedule = service.schedule ? service.schedule : '-';
      var serviceRole =  service.role ? service.role : '-';
      var serviceActive = service.active ? service.active : '-';
      var serviceClass = service.class ? service.class : '-';
      var serviceUtilities = getFormattedUtilities(service);
      var serviceUtilities = serviceUtilities ? serviceUtilities : '<div class="fillWidth textAlignCenter">This service has no utilities</div>';
      var htmlMiddle = '<div class="fillWidth padding15">' +
                          '<div class="row margin0">' +
                            '<div class="col-lg-6 col-md-6 col-sm-6 col-xs-6 marginTop15">' +
                              '<div>schedule:</div>' +
                              '<div>role:</div>' +
                              '<div>active:</div>' +
                              '<div>class:</div>' +
                            '</div>' +
                            '<div class="col-lg-6 col-md-6 col-sm-6 col-xs-6 marginTop15">' +
                              '<div>' + serviceSchedule + '</div>' +
                              '<div>' + serviceRole + '</div>' +
                              '<div>' + serviceActive + '</div>' +
                              '<div>' + serviceClass + '</div>' +
                            '</div>' +
                            '<div class="clearBoth fillWidth textAlignCenter padding15">Params</div>' +
                            tableWrapper +
                            '<div class="fillWidth textAlignCenter padding15">Utilities</div>' +
                            serviceUtilities +
                          '</div>' +
                        '</div>';
      var box = '<div class="col-lg-12 col-md-12 col-sm-12 col-xs-12 padding15">' +
                  '<div class="fillParent boxShadow colorBlack borderRadius whiteBackground padding0">' +
                    htmlTop +
                    htmlMiddle +
                  '</div>' +
                '</div>';
      return box;
    }

    function getFormattedParams(service){
      var htmlParams = null;
      switch (service.name) {
        case 'PulitoreMongoDownload':
          htmlParams =  '<tr><td>interval</td><td>' + service.params.interval + '</td></tr>' +
                        '<tr><td>connectUri</td><td>' + service.params.connectUri + '</td></tr>';
          break;
        case 'AzzeratoreUidEmailScaricate':
          htmlParams =  '<tr><td>driver</td><td>' + service.params.driver + '</td></tr>' +
                        '<tr><td>connectUri</td><td>' + service.params.connectUri + '</td></tr>';
          break;
        case 'PulitoreCestinoMongo':
          htmlParams =  '<tr><td>intervalDays</td><td>' + service.params.intervalDays + '</td></tr>' +
                        '<tr><td>connectUri</td><td>' + service.params.connectUri + '</td></tr>';
          break;
        case 'Spedizioniere':
          htmlParams =  '<tr><td>timeOutHours</td><td>' + service.params.timeOutHours + '</td></tr>' +
                        '<tr><td>maxThread</td><td>' + service.params.maxThread + '</td></tr>' +
                        '<tr><td>expired</td><td>' + service.params.expired + '</td></tr>' +
                        '<tr><td>spedizioniereUrl</td><td>' + service.params.spedizioniereUrl + '</td></tr>' +
                        '<tr><td>username</td><td>' + service.params.username + '</td></tr>' +
                        '<tr><td>password</td><td>' + service.params.password + '</td></tr>' +
                        '<tr><td>testMode</td><td>' + service.params.testMode + '</td></tr>';
          break;
        case 'VersatoreParer':
          htmlParams =  '<tr><td>idApplicazione</td><td>' + service.params.idApplicazione + '</td></tr>' +
                        '<tr><td>tokenApplicazione</td><td>' + service.params.tokenApplicazione + '</td></tr>' +
                        '<tr><td>canSendPicoUscita</td><td>' + service.params.canSendPicoUscita + '</td></tr>' +
                        '<tr><td>canSendPicoEntrata</td><td>' + service.params.canSendPicoEntrata + '</td></tr>' +
                        '<tr><td>canSendDete</td><td>' + service.params.canSendDete + '</td></tr>' +
                        '<tr><td>canSendDeli</td><td>' + service.params.canSendDeli + '</td></tr>' +
                        '<tr><td>canSendRegistroGiornaliero</td><td>' + service.params.canSendRegistroGiornaliero + '</td></tr>' +
                        '<tr><td>canSendRgPico</td><td>' + service.params.canSendRgPico + '</td></tr>' +
                        '<tr><td>canSendRgDete</td><td>' + service.params.canSendRgDete + '</td></tr>' +
                        '<tr><td>canSendRgDeli</td><td>' + service.params.canSendRgDeli + '</td></tr>' +
                        '<tr><td>canSendRegistroAnnuale</td><td>' + service.params.canSendRegistroAnnuale + '</td></tr>' +
                        '<tr><td>limit</td><td>' + service.params.limit + '</td></tr>' +
                        '<tr><td>versione</td><td>' + service.params.versione + '</td></tr>' +
                        '<tr><td>ambiente</td><td>' + service.params.ambiente + '</td></tr>' +
                        '<tr><td>ente</td><td>' + service.params.ente + '</td></tr>' +
                        '<tr><td>strutturaVersante</td><td>' + service.params.strutturaVersante + '</td></tr>' +
                        '<tr><td>userID</td><td>' + service.params.userID + '</td></tr>' +
                        '<tr><td>tipoComponenteDefault</td><td>' + service.params.tipoComponenteDefault + '</td></tr>' +
                        '<tr><td>codifica</td><td>' + service.params.codifica + '</td></tr>' +
                        '<tr><td>useFakeId</td><td>' + service.params.useFakeId + '</td></tr>';
          break;
      }
      if (htmlParams) {
        var tableWrapper =  '<div class="fillWidth table-responsive">' +
                              '<table class="table">' +
                                '<thead>' +
                                  '<th>Key</th>' +
                                  '<th>Value</th>' +
                                '</thead>' +
                                '<tbody>' +
                                  htmlParams +
                                '</tbody>' +
                              '</table>' +
                            '</div>';
        return tableWrapper;
      } else {
        return htmlParams;
      }
    }

    function getFormattedUtilities(service){
      var htmlUtilities = null;
      switch (service.name) {
        case 'VersatoreParer':
          htmlUtilities = '<div class="fillWidth">' +
                            '<button id="showGuiInputGuid" type="button" class="btn btn-primary col-lg-2 col-md-3 col-sm-4 col-xs-6">Versa ora!</button>' +
                          '</div>';
          break;
      }
      return htmlUtilities;
    }

    function attacheFunctionsToButtons(sevice){
      switch (service.name) {
        case 'VersatoreParer':
          var innerBox =  '<textarea id="guidList" class="form-control borderRadius" placeholder="Inserire qui i guid in formato JSON. Esempio: [&quot;guid_gddoc1&quot;,&quot;guid_gddoc2&quot;]" rows="10"></textarea>' +
                          '<div class="row">' +
                            '<button id="fireVersatoreParer" type="button" class="btn btn-link noDecoretionOnHover col-lg-6 col-md-6 col-sm-6 col-xs-6">Versa</button>' +
                            '<button id="closeVersatoreParer" type="button" class="btn btn-link noDecoretionOnHover col-lg-6 col-md-6 col-sm-6 col-xs-6">Annulla</button>' +
                          '</div>';
          $('#showGuiInputGuid').click(function(){
              var box = formatPopUp('col-lg-6 col-md-6 col-sm-8 col-xs-10', innerBox);
              showPopUp(box);
              $('#guidList').click(function(event){
                  event.stopPropagation();
              });
              $('#fireVersatoreParer').click(function(event){
                  event.stopPropagation();
                  var content = $('#guidList').val();
                  fireService(service.name, content)
              });
              $('#closeVersatoreParer').click(function(event){
                  event.stopPropagation();
                  hidePopUp();
              });
            });

          break;
      }
    }

    function formatPopUp(classes, popUpContent){
      var box = '<div id="wrapperButtons" class="snsBox fillParent">' +
                  '<div class="innerSnsBox fillParent">' +
                    '<div class="displayInlineBlock floatNone padding0 borderRadius boxShadow whiteBackground colorBlack '+ classes +'">' +
                      popUpContent +
                    '</div>' +
                  '</div>' +
                '</div>';
      return box;
    }

    function showPopUp(box){
      $('body').attr('style', 'position:fixed');
      $('#mainContent').append(box);
      var element = document.getElementById('wrapperButtons');
      $('#wrapperButtons').click(function(event){
        if ($(event.currentTarget).attr('id') === 'wrapperButtons') {
          hidePopUp();
        }
      });
    }

    function hidePopUp(){
      var element = document.getElementById('wrapperButtons');
      $('body').attr('style', '');
      removeElemnt(element);
    }

    function fireService(serviceName, content){
      if (content) {
        $.ajax({
          method: 'POST',
          url: servletUrl,
          data: {
                  'schedulatore': 'fireService',
                  'service': serviceName,
                  'content': content
                },
          success: function(data){
            hidePopUp();
            showTemporaryMessage('Versamento effettuato con successo!');
          },
          error: function(jqXHR, textStatus, errorThrown){
            showTemporaryMessage('Versamento Fallito!</br>Consultare i log per maggiori informazioni.');
            console.error(jqXHR, textStatus, errorThrown);
          }
        });
      }else {
        showTemporaryMessage("Inserire almeno un Guid per continuare!");
      }
    }

    function getFormattedButtons(active){
      var options = '';
      if (active) {
        options = '<a href="#">' +
                    '<div id="shutdown" class="col-lg-6 col-md-6 col-sm-6 col-xs-6 padding30">' +
                      '<svg class="glyShutdown" fill="#000000" height="24" viewBox="0 0 24 24" width="24" xmlns="http://www.w3.org/2000/svg">' +
                        '<path d="M0 0h24v24H0z" fill="none"/>' +
                        '<path d="M13 3h-2v10h2V3zm4.83 2.17l-1.42 1.42C17.99 7.86 19 9.81 19 12c0 3.87-3.13 7-7 7s-7-3.13-7-7c0-2.19 1.01-4.14 2.58-5.42L6.17 5.17C4.23 6.82 3 9.26 3 12c0 4.97 4.03 9 9 9s9-4.03 9-9c0-2.74-1.23-5.18-3.17-6.83z"/>' +
                      '</svg>' +
                      '<span class="displayBlock paddingTop8">Shutdown</span>' +
                    '</div>' +
                    '<div id="reboot" class="col-lg-6 col-md-6 col-sm-6 col-xs-6 padding30">' +
                      '<svg class="glyReboot" fill="#000000" height="24" viewBox="0 0 24 24" width="24" xmlns="http://www.w3.org/2000/svg">' +
                        '<path d="M0 0h24v24H0z" fill="none"/>' +
                        '<path d="M13 3h-2v10h2V3zm4.83 2.17l-1.42 1.42C17.99 7.86 19 9.81 19 12c0 3.87-3.13 7-7 7s-7-3.13-7-7c0-2.19 1.01-4.14 2.58-5.42L6.17 5.17C4.23 6.82 3 9.26 3 12c0 4.97 4.03 9 9 9s9-4.03 9-9c0-2.74-1.23-5.18-3.17-6.83z"/>' +
                      '</svg>' +
                      '<span class="displayBlock paddingTop8">Reboot</span>' +
                    '</div>' +
                  '</a>';
      }else {
        options = '<a href="#">' +
                    '<div id="start" class="fillParent padding30">' +
                      '<svg class="glyStart" fill="#000000" height="24" viewBox="0 0 24 24" width="24" xmlns="http://www.w3.org/2000/svg">' +
                        '<path d="M0 0h24v24H0z" fill="none"/>' +
                        '<path d="M13 3h-2v10h2V3zm4.83 2.17l-1.42 1.42C17.99 7.86 19 9.81 19 12c0 3.87-3.13 7-7 7s-7-3.13-7-7c0-2.19 1.01-4.14 2.58-5.42L6.17 5.17C4.23 6.82 3 9.26 3 12c0 4.97 4.03 9 9 9s9-4.03 9-9c0-2.74-1.23-5.18-3.17-6.83z"/>' +
                      '</svg>' +
                      '<span class="displayBlock paddingTop8">Start</span>' +
                    '</div>' +
                  '</a>';
      }
      var box = '<div id="wrapperButtons" class="snsBox fillParent">' +
                  '<div class="innerSnsBox fillParent">' +
                    '<div class="displayInlineBlock col-lg-2 col-md-2 col-sm-3 col-xs-4 floatNone padding0 borderRadius boxShadow whiteBackground colorBlack">' +
                      options +
                    '</div>' +
                  '</div>' +
                '</div>';
      return box;
    }

    function startAndStop(status){
      var message = '';
      if (status === 'stop') {
        message = 'All services are being stopped. Please reload the page in a few second.'
      } else if (status === 'start') {
        message = 'Starting all the services. Please reload the page in a few second.'
      } else{
        message = 'All services are being reloaded. Please reload the page in a few second.'
      }
      $.ajax({
        method: 'POST',
        url: servletUrl,
        data: {schedulatore: status},
        success: function(data){
          $('#mainContent').text(message);
        },
        error: function(jqXHR, textStatus, errorThrown){
          console.error(jqXHR, textStatus, errorThrown);
        }
      });
    }

    function showStartAndStop(){
      $.ajax({
        method: 'POST',
        url: servletUrl,
        data: {schedulatore: 'status'},
        success: function(data){
          var box = getFormattedButtons(data.active);
          showPopUp(box);
          $('#shutdown').click(function(){startAndStop('stop')});
          $('#start').click(function(){startAndStop('start')});
          $('#reboot').click(function(){startAndStop('reload')});
        },
        error: function(jqXHR, textStatus, errorThrown){
          console.error(jqXHR, textStatus, errorThrown);
        }
      });
      return null;
    }

    function removeElemnt(element){
      element.parentElement.removeChild(element);
    }

    function changeServiceStatus(serviceIndex){ // non uso gli indici perchè il js essendo asincrono viene passato alla funzione sempre l'ultimo indice
      //console.log('In: ' + serviceIndex);
      var newStatus= allServices.jobs[serviceIndex].active === 'true'? 'false' : 'true';
      allServices.jobs[serviceIndex].active = newStatus;
      // var serviceName = '';
      // for (service of allServices.jobs) {
      //   if (service.name === serviceName) {
      //     newStatus =
      //     service.active = newStatus;
      //     serviceName = service.name;
      //     break;
      //   }
      // }
      var jsonToString = JSON.stringify(allServices, null, '\t');
      $.ajax({
        method: 'POST',
        url: servletUrl,
        data: {
                service: allServices.jobs[serviceIndex].name,
                json: jsonToString,
                status: newStatus
              },
        success: function(){
          // showTemporaryMessage('Lo stato del servizio è stato cambiato con successo!<br>Aggiornare la pagina per avere lo stato attuale dei servizi.');
          showAllServices(allServices, 1);
        },
        error: function(jqXHR, textStatus, errorThrown){
          console.error(jqXHR, textStatus, errorThrown);
        }
      });
    }

    $('#dashboardIcon').click(function(){showAllServices()});
    $('#allServices').click(function(){showAllServices()});
    $('#activeServices').click(function(){showActiveServices(true)});
    $('#stoppedServices').click(function(){showActiveServices(false)});
    $('#startAndStop').click(function(){showStartAndStop()});

    showAllServices();
  });

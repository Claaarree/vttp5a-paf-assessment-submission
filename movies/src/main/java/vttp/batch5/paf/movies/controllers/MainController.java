package vttp.batch5.paf.movies.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.json.JsonArray;
import vttp.batch5.paf.movies.services.MovieService;

@RestController
@RequestMapping("/api")
public class MainController {
  
  @Autowired
  private MovieService movieService;

  // TODO: Task 3
  @GetMapping(path = "/summary", produces = "application/json")
  public ResponseEntity<String> getProlificDirectors(@RequestParam int count) {
    JsonArray jArray = movieService.getProlificDirectors(count);

    return ResponseEntity.ok().body(jArray.toString());
  }
   

  
  // TODO: Task 4


}
